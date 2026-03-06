"""
parameter_tracker.py - 전략 파라미터 실시간 추적 및 DB 적재 모듈
[V19] 오라클 서버에서 24시간 파라미터 최적화를 위해 데이터를 Supabase DB에 적재
"""

import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from database import StrategyParameters, AsyncSessionLocal
from config import settings, logger


class ParameterTracker:
    """
    전략 파라미터의 실시간 추적 및 DB 저장을 담당하는 클래스
    
    주요 기능:
    1. 거래 시점의 전체 파라미터 스냅샷 저장
    2. 파라미터 해시값 생성 및 중복 검사
    3. 성과 지표 업데이트
    4. 파라미터 최적화를 위한 데이터 제공
    """
    
    def __init__(self):
        self.current_parameters: Dict[str, Any] = {}
        self.parameter_hash: str = ""
        self._load_current_parameters()
    
    def _load_current_parameters(self):
        """현재 설정된 모든 전략 파라미터를 로드합니다."""
        self.current_parameters = {
            # 기본 전략 파라미터
            "strategy_version": getattr(settings, "STRATEGY_VERSION", "UNKNOWN"),
            "timeframe": getattr(settings, "TIMEFRAME", "3m"),
            "risk_percentage": getattr(settings, "RISK_PERCENTAGE", None),
            "leverage": getattr(settings, "LEVERAGE", None),
            
            # ATR 및 변동성 파라미터
            "atr_ratio_mult": getattr(settings, "ATR_RATIO_MULT", None),
            "atr_long_len": getattr(settings, "ATR_LONG_LEN", None),
            
            # 손익 관리 파라미터
            "sl_mult": getattr(settings, "SL_MULT", None),
            "tp_mult": getattr(settings, "TP_MULT", None),
            "chandelier_mult": getattr(settings, "CHANDELIER_MULT", None),
            "chandelier_atr_len": getattr(settings, "CHANDELIER_ATR_LEN", None),
            "breakeven_trigger_mult": getattr(settings, "BREAKEVEN_TRIGGER_MULT", None),
            "breakeven_profit_mult": getattr(settings, "BREAKEVEN_PROFIT_MULT", None),
            
            # V18 스코어링 엔진 파라미터
            "min_entry_score": getattr(settings, "MIN_ENTRY_SCORE", None),
            "pctl_window": getattr(settings, "PCTL_WINDOW", None),
            "adx_boost_pctl": getattr(settings, "ADX_BOOST_PCTL", None),
            "scoring_thresholds": getattr(settings, "SCORING_THRESHOLDS", {}),
            
            # 포트폴리오 관리 파라미터
            "max_concurrent_same_dir": getattr(settings, "MAX_CONCURRENT_SAME_DIR", None),
            "max_trades": getattr(settings, "MAX_TRADES", None),
            "loss_cooldown_minutes": getattr(settings, "LOSS_COOLDOWN_MINUTES", None),
            
            # 체결 관리 파라미터
            "kelly_sizing": getattr(settings, "KELLY_SIZING", False),
            "kelly_min_trades": getattr(settings, "KELLY_MIN_TRADES", None),
            "kelly_max_fraction": getattr(settings, "KELLY_MAX_FRACTION", None),
            "chasing_wait_sec": getattr(settings, "CHASING_WAIT_SEC", None),
            "partial_tp_ratio": getattr(settings, "PARTIAL_TP_RATIO", None),
            
            # MTF 필터 파라미터
            "htf_timeframe_1h": getattr(settings, "HTF_TIMEFRAME_1H", None),
            "htf_timeframe_15m": getattr(settings, "HTF_TIMEFRAME_15M", None),
        }
        
        # 파라미터 해시값 생성
        param_str = json.dumps(self.current_parameters, sort_keys=True)
        self.parameter_hash = hashlib.sha256(param_str.encode()).hexdigest()
    
    async def save_parameters_to_db(self, symbol: str = "ALL") -> Optional[int]:
        """
        현재 파라미터 셋을 DB에 저장합니다.
        
        Args:
            symbol: 종목 심볼 (기본값: "ALL")
            
        Returns:
            저장된 레코드의 ID 또는 None (중복 시)
        """
        try:
            async with AsyncSessionLocal() as session:
                # 동일한 파라미터 해시가 이미 있는지 확인
                stmt = select(StrategyParameters).where(
                    StrategyParameters.parameter_hash == self.parameter_hash,
                    StrategyParameters.symbol == symbol,
                    StrategyParameters.is_active == True
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()
                
                if existing:
                    logger.debug(f"[ParameterTracker] 동일 파라미터 셋 이미 존재: {self.parameter_hash[:16]}...")
                    return existing.id
                
                # 새 파라미터 레코드 생성
                new_param = StrategyParameters(
                    timestamp=datetime.utcnow(),
                    symbol=symbol,
                    parameters=self.current_parameters,
                    strategy_version=self.current_parameters.get("strategy_version", "UNKNOWN"),
                    parameter_hash=self.parameter_hash,
                    is_active=True
                )
                
                session.add(new_param)
                await session.commit()
                await session.refresh(new_param)
                
                logger.info(
                    f"[ParameterTracker] 새 파라미터 셋 저장 완료 | "
                    f"ID: {new_param.id} | Symbol: {symbol} | "
                    f"Version: {self.current_parameters.get('strategy_version')}"
                )
                
                return new_param.id
                
        except Exception as e:
            logger.error(f"[ParameterTracker] 파라미터 DB 저장 실패: {e}")
            return None
    
    async def update_performance_metrics(
        self, 
        parameter_id: int, 
        trade_count: int = 0,
        win_rate: float = 0.0,
        avg_return: float = 0.0,
        sharpe_ratio: float = 0.0
    ):
        """
        특정 파라미터 셋의 성과 지표를 업데이트합니다.
        
        Args:
            parameter_id: 파라미터 레코드 ID
            trade_count: 총 거래 횟수
            win_rate: 승률 (0.0 ~ 1.0)
            avg_return: 평균 수익률
            sharpe_ratio: 샤프 지수
        """
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(StrategyParameters).where(
                    StrategyParameters.id == parameter_id
                )
                result = await session.execute(stmt)
                param_record = result.scalar_one_or_none()
                
                if param_record:
                    param_record.trade_count = trade_count
                    param_record.win_rate = win_rate
                    param_record.avg_return = avg_return
                    param_record.sharpe_ratio = sharpe_ratio
                    param_record.updated_at = datetime.utcnow()
                    
                    await session.commit()
                    logger.debug(
                        f"[ParameterTracker] 성과 지표 업데이트 완료 | "
                        f"ID: {parameter_id} | Trades: {trade_count} | WinRate: {win_rate:.2%}"
                    )
                    
        except Exception as e:
            logger.error(f"[ParameterTracker] 성과 지표 업데이트 실패: {e}")
    
    async def get_active_parameters(self, symbol: str = None) -> list:
        """
        활성화된 파라미터 셋 목록을 조회합니다.
        
        Args:
            symbol: 특정 종목 필터 (선택사항)
            
        Returns:
            StrategyParameters 객체 리스트
        """
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(StrategyParameters).where(
                    StrategyParameters.is_active == True
                )
                
                if symbol:
                    stmt = stmt.where(StrategyParameters.symbol == symbol)
                    
                stmt = stmt.order_by(StrategyParameters.created_at.desc())
                
                result = await session.execute(stmt)
                return result.scalars().all()
                
        except Exception as e:
            logger.error(f"[ParameterTracker] 활성 파라미터 조회 실패: {e}")
            return []
    
    def get_parameters_json(self) -> str:
        """현재 파라미터를 JSON 문자열로 반환합니다."""
        return json.dumps(self.current_parameters, ensure_ascii=False, indent=2)
    
    def get_parameter_hash(self) -> str:
        """현재 파라미터 해시값을 반환합니다."""
        return self.parameter_hash
    
    def refresh_parameters(self):
        """설정 변경 시 파라미터를 새로고침합니다."""
        old_hash = self.parameter_hash
        self._load_current_parameters()
        
        if old_hash != self.parameter_hash:
            logger.info(
                f"[ParameterTracker] 파라미터 변경 감지 | "
                f"Old: {old_hash[:16]}... | New: {self.parameter_hash[:16]}..."
            )
            return True
        return False


# 전역 인스턴스 생성
parameter_tracker = ParameterTracker()
