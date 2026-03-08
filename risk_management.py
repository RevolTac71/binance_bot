from config import settings, logger
from database import TradeLog, AsyncSessionLocal
from sqlalchemy.future import select


class RiskManager:
    def __init__(self, data_pipeline):
        """
        V18 리스크 매니지먼트 (Half-Kelly + Risk Parity 하이브리드)
        - KELLY_SIZING=True 시: 최근 거래 기록의 승률(p)과 손익비(b)로 투입 비중 자동 계산
        - KELLY_SIZING=False 시: 기존 RISK_PERCENTAGE 고정 비율 방식 유지
        """
        self.pipeline = data_pipeline
        self.risk_pct = settings.RISK_PERCENTAGE  # 허용 손실 비율 (예: 0.005)
        self.leverage = settings.LEVERAGE
        self.min_order_usdt = 6.0  # 바이낸스 선물 최소 주문 금액 방어망

        # Kelly 캐시 (매 거래마다 DB 조회를 방지하기 위해 주기적 갱신)
        self._kelly_cache = None
        self._kelly_cache_count = 0

    async def _fetch_recent_stats(self, min_trades: int = 20) -> tuple:
        """
        최근 거래 기록에서 승률(p)과 평균 손익비(b)를 산출합니다.
        Returns: (win_rate, avg_win_loss_ratio) 또는 데이터 부족 시 (None, None)
        """
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(TradeLog.realized_pnl)
                    .where(TradeLog.realized_pnl.isnot(None))
                    .order_by(TradeLog.exit_time.desc())
                    .limit(100)
                )
                pnls = [row[0] for row in result.fetchall()]

            if len(pnls) < min_trades:
                logger.info(
                    f"[Kelly] 표본 부족 ({len(pnls)}/{min_trades}). "
                    f"고정 비율({self.risk_pct * 100:.1f}%) 폴백."
                )
                return None, None

            wins = [p for p in pnls if p > 0]
            losses = [abs(p) for p in pnls if p < 0]

            if not losses or not wins:
                return None, None

            # 승률과 평균 손익비 산출
            p = len(wins) / len(pnls)
            b = (sum(wins) / len(wins)) / (sum(losses) / len(losses))

            logger.info(
                f"[Kelly] 표본={len(pnls)}, 승률={p:.2%}, "
                f"손익비={b:.2f}, 순수 Kelly={(p * (b + 1) - 1) / b:.4f}"
            )
            return p, b
        except Exception as e:
            logger.error(f"[Kelly] 거래 기록 조회 중 에러: {e}")
            return None, None

    def _half_kelly(self, p: float, b: float) -> float:
        """
        Half-Kelly 비율 산출.
        공식: f* = (p(b+1) - 1) / b
        꼬리 위험 방어를 위해 산출값의 절반 사용.
        """
        kelly = (p * (b + 1) - 1) / b
        kelly = max(0.0, kelly)  # 음수면 베팅하지 않음 (손실 기대)
        half_kelly = kelly / 2  # 꼬리 위험 방어

        # 최대 투입 비율 캡 적용
        max_frac = getattr(settings, "KELLY_MAX_FRACTION", 0.05)
        result = min(half_kelly, max_frac)

        logger.info(
            f"[Kelly] 풀 Kelly={kelly:.4f}, "
            f"Half-Kelly={half_kelly:.4f}, 캡 적용={result:.4f}"
        )
        return result

    async def calculate_position_size(
        self,
        symbol: str,
        capital: float,
        entry_price: float,
        atr_val: float,
        entry_type: str = "TREND_MACD",
    ) -> dict:
        """
        V18 하이브리드 사이징: 진입 유형에 따른 차등 SL/TP 적용.
        - TREND_MACD: SL 3.0x / TP 5.0x (v18 추천)
        - SCALP_CVD: SL 1.0x / TP 5.0x (v18 추천)
        """
        if capital <= 0 or entry_price <= 0 or atr_val <= 0:
            return {"size": 0.0, "invest_usdt": 0.0, "tp_dist": 0.0, "sl_dist": 0.0}

        # 1. 진입 유형별 SL/TP 배율 설정 (v18 전략 권장값)
        # 만약 settings에 명시적으로 설정된 값이 있다면 그것을 우선하되, 기본값은 유형별 차등 적용
        sl_mult = getattr(
            settings, "SL_MULT", (3.0 if entry_type == "TREND_MACD" else 1.0)
        )
        tp_mult = getattr(settings, "TP_MULT", 6.0)

        # Kelly 사이징 활성 시 비중 조절
        risk_pct = self.risk_pct
        if getattr(settings, "KELLY_SIZING", False):
            min_trades = getattr(settings, "KELLY_MIN_TRADES", 20)
            p, b = await self._fetch_recent_stats(min_trades)
            if p is not None and b is not None:
                risk_pct = self._half_kelly(p, b)
                if risk_pct <= 0:
                    risk_pct = self.risk_pct

        # 2. 1회 투입 증거금 액수 산출
        margin_invest = capital * risk_pct

        # 3. 거래당 스탑폭/익절폭 금액 산출
        sl_distance = atr_val * sl_mult
        tp_distance = atr_val * tp_mult

        # 4. 최대 레버리지를 곱한 명목 진입 금액 (Notional Value)
        notional_value = margin_invest * self.leverage

        # 4. 바이낸스 최소 주문 한도(5.5~6.0 USDT) 방어
        if notional_value < self.min_order_usdt:
            notional_value = self.min_order_usdt
            margin_invest = notional_value / self.leverage

        # 5. 가용 증거금 초과(풀시드 초과) 안전장치 (가용 잔고의 95%까지만 최대 허용)
        if margin_invest > capital * 0.95:
            margin_invest = capital * 0.95
            notional_value = margin_invest * self.leverage

        # 6. 최종 계약 수량 산정
        calc_size = notional_value / entry_price

        # 수량 정밀도 포맷 관리 (바이낸스 규격 소수점)
        try:
            sz_str = self.pipeline.exchange.amount_to_precision(symbol, calc_size)
            final_size = float(sz_str)
        except Exception as e:
            final_size = calc_size

        # 실제 투입 증거금 재계산
        actual_margin_invest = (final_size * entry_price) / self.leverage

        # 예상되는 달러 손절 금액 (수량 * 스탑폭)
        expected_loss = final_size * sl_distance

        sizing_method = (
            "Half-Kelly"
            if (getattr(settings, "KELLY_SIZING", False) and risk_pct != self.risk_pct)
            else "고정비율"
        )
        logger.info(
            f"[Position Sizing] {symbol} | 방식: {sizing_method} ({risk_pct * 100:.2f}%) | "
            f"실투입 증거금: {actual_margin_invest:.2f} USDT | 수량: {final_size} | "
            f"TP: +{tp_distance:.4f} / SL: -{sl_distance:.4f} (예상손실: {expected_loss:.2f} USDT)"
        )

        return {
            "size": final_size,
            "invest_usdt": actual_margin_invest,
            "tp_dist": tp_distance,
            "sl_dist": sl_distance,
        }
