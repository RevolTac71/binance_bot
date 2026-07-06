# 📈 Bollinger Bands Squeeze & Volatility Breakout 전략 명세서 (V18.7)

본 문서는 바이낸스 선물 롱/숏 퀀트 봇(`binance_bot`)에 탑재된 볼린저 밴드 수축(Squeeze) 및 변동성 돌파(Volatility Breakout) 전략의 논리 구조, 수학적 연산 공식 및 V18.7 고도화 필터에 대한 세부 명세서입니다.

---

## 1. 개요 (Overview)

변동성은 항상 고정되어 있지 않고 **"수축과 팽창(Volatility Clustering)"**을 반복하는 성질이 있습니다. 본 전략은 시장의 변동성이 극도로 낮아진 수축(Squeeze) 구간에 진입한 대상을 선별하고, 이후 임계 거래량을 동반하여 수축 구간을 강하게 이탈(Breakout)하는 방향으로 빠르게 편승하여 수익을 내는 **추세 추종 돌파 전략**입니다.

```
  변동성 수축 (Squeeze)             변동성 폭발 (Breakout)
   ┌───────────────┐                 ┌───────────────┐
   │               │                 │       ▲ (LONG 진입)
   │  Bollinger    │                 │      ╱ 
───┼─── Bands ─────┼─── 수축 ────────┼─────╱─────────
   │  Narrowing    │                 │
   │               │                 │
   └───────────────┘                 └───────────────┘
```

---

## 2. 세부 수학 공식 및 기술 지표 (Mathematical Indicators)

### 2.1 볼린저 밴드 (Bollinger Bands)
* **중심선 (Middle Band)**: \(N\)기간 동안의 종가 단순이동평균(SMA)
  \[MB_t = \frac{1}{N} \sum_{i=0}^{N-1} Close_{t-i}\]
* **상단선 (Upper Band)**: 중심선 + 종가의 \(K\)배 표준편차
  \[UB_t = MB_t + (K \times \sigma_t)\]
* **하단선 (Lower Band)**: 중심선 - 종가의 \(K\)배 표준편차
  \[LB_t = MB_t - (K \times \sigma_t)\]
  *(전략 기본값: \(N = 20\), \(K = 2.0\))*

### 2.2 밴드폭 및 100봉 백분위수 수축 판정 (Percentile Bandwidth Squeeze)
* **밴드폭 (Bandwidth)**: 상하단선 간의 이격 폭
  \[BW_t = UB_t - LB_t\]
* **밴드폭 백분위수 (Percentile of Score)**:
  최근 100봉 동안의 밴드폭 집합 \(\{BW_{t-99}, BW_{t-98}, \dots, BW_t\}\) 중에서 현재 밴드폭 \(BW_t\)의 백분위 등급 \(P_t\)를 산출합니다.
  \[P_t = \text{percentileofscore}(\{BW_{t-99}, \dots, BW_t\}, BW_t)\]
* **정석적 스퀴즈 판정 조건**:
  현재 밴드폭이 최근 100봉 동안의 밴드폭 중 **하위 20% 미만**으로 좁아졌는지를 감시합니다.
  \[P_t < 20.0\]

---

## 3. 매매 로직 및 신호 처리 (Trading Logic & Signals)

### 3.1 수축 진입 단계 (Squeeze Entry Stage)
* **스퀴즈 조건 만족**: 현재 봉의 밴드폭 백분위수 등급 \(P_t < 20.0\) 일 때, 전략 상태기(`ticker_status`)가 `in_squeeze = True`로 전환되며 수축 대기 카운트(`squeeze_bars = 1`)가 누적되기 시작합니다.
* 데이터 개수가 부족하여 100봉 백분위수 산출이 불가능할 경우(50봉 미만), 기존의 평균 밴드폭 기반 수축 비율 필터로 폴백합니다.
  \[BW_t \le \text{Mean\_Bandwidth}_t \times 0.8\]

### 3.2 진입 판단 단계 (Breakout Entry Stage)
수축 카운트가 활성화된 상태에서 최대 **12봉(`MAX_SQUEEZE_DURATION`)** 이내에 아래 두 조건을 동시에 만족할 때 진입 시그널을 활성화합니다.

#### 1) 종가 돌파 검증 (Price Breakout)
* **LONG 진입**: 현재 종가가 볼린저 밴드 상단선보다 높은 경우
  \[Close_t > UB_t\]
* **SHORT 진입**: 현재 종가가 볼린저 밴드 하단선보다 낮은 경우
  \[Close_t < LB_t\]

#### 2) 거래량 폭발 검증 (Volume Burst)
돌파 시점에 시장의 합의와 모멘텀이 실렸는지 검증하기 위해 20봉 평균 거래량 대비 임계 비율을 확인합니다.
* **거래량 조건**:
  \[Volume_t > \left( \frac{1}{20} \sum_{i=1}^{20} Volume_{t-i} \right) \times Volume\_Multiplier \quad (기본값: 2.0)\]

#### 3) 만료 및 예외 처리 (Timeout & Reset)
* 스퀴즈 돌입 이후 **12봉**이 경과할 때까지 돌파 및 거래량 폭발이 발생하지 않으면, 기존 스퀴즈 상태는 무효화(`in_squeeze = False`, `squeeze_bars = 0`)되어 대기 모드로 돌아갑니다.

---

## 4. 실전 필터 결합 (Actionable Filters)

속임수 돌파(False Breakout) 방지를 위해 아래 2가지 실전 필터 조건을 기존 전략 진입 로직에 `AND` 조건으로 추가 결합합니다.

### 4.1 펀딩비 모멘텀 필터 (Funding Rate Filter)
* **기능**: 바이낸스 API(`fetch_funding_rate`)를 통해 실시간 펀딩비(Funding Rate)를 조회하여 과열 정국 여부를 감시합니다.
* **차단 제약 조건**:
  - **LONG 진입 차단**: 실시간 펀딩비가 극단적인 양수일 때 (상승 과열로 인한 숏스퀴즈/조정 위험)
    \[Funding\_Rate > 0.1\% \quad (0.001)\]
  - **SHORT 진입 차단**: 실시간 펀딩비가 극단적인 음수일 때 (하락 과열로 인한 숏스퀴즈 반등 위험)
    \[Funding\_Rate < -0.05\% \quad (-0.0005)\]

### 4.2 볼륨 프로파일 매물대 필터 (Volume Profile POC Filter)
* **기능**: 최근 200봉의 가격-거래량 기준 매물 집중대인 POC(Point of Control)를 계산한 후 저항 및 지지 여부를 체크합니다.
* **POC 산출**: 200개 캔들의 고가와 저가 범위를 50개의 균등 구간(Bins)으로 분할하고, 각 구간에 속한 캔들의 거래량 누적합을 구해 최대 거래량이 쌓인 구간의 중간 가격을 POC 가격(\(Price_{POC}\))으로 산출합니다.
* **차단 제약 조건**:
  - **LONG 진입 차단**: 종가 바로 위에 최대 매물대 저항선이 인접해 있는 경우
    \[Close_t < Price_{POC} \le Close_t \times 1.02\]
  - **SHORT 진입 차단**: 종가 바로 아래에 최대 매물대 지지선이 인접해 있는 경우
    \[Close_t \times 0.98 \le Price_{POC} < Close_t\]

---

## 5. 포지션 청산 설계 (Chandelier Exit Safety Net)

진입 완료 시 ATR(Average True Range)을 기반으로 동적 청산 및 트레일링 스탑 라인을 추적합니다.

### 5.1 최초 TP/SL 설정
* **ATR 산출**: 최근 14봉의 고가-저가 변동 폭 평균
* **TP (익절가)**: \[\text{Entry Price} \pm (\text{ATR} \times \text{ATR\_Multiplier})\]
* **SL (손절가)**: \[\text{Entry Price} \mp (\text{ATR} \times \text{ATR\_Multiplier})\]

### 5.2 샹들리에 추적 손절선 (Chandelier Exit)
포지션 진입 이후 가격이 당초 예상 방향으로 유리하게 움직일 경우, 손실폭을 감쇄하고 수익을 지키기 위해 **샹들리에 익절/손절선**을 동적으로 올려(내려) 잡습니다.

* **LONG 포지션**:
  \[Stop\_Price_t = \max(Stop\_Price_{t-1}, \, \text{Highest\_High}_{t} - (\text{ATR}_t \times \text{Chandelier\_Multiplier}))\]
* **SHORT 포지션**:
  \[Stop\_Price_t = \min(Stop\_Price_{t-1}, \, \text{Lowest\_Low}_{t} + (\text{ATR}_t \times \text{Chandelier\_Multiplier}))\]
  *(샹들리에 기본 승수: 3.0)*

가장 유리했던 최고점/최저점을 기준으로 가격이 일정 수준 역행하여 샹들리에 라인을 돌파 터치하면, 봇은 즉시 포지션을 시장가로 전량 정리(`Chandelier Exit 청산`)합니다.
