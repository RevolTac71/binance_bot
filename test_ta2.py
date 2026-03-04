import pandas as pd
import pandas_ta as ta

df = pd.DataFrame({"ADX_14": [float("nan")] * 100})
try:
    df["ADX_SMA_50"] = ta.sma(df["ADX_14"], length=50)
    print("ta.sma with NaN Series worked")
except Exception as e:
    print("Error 1:", type(e).__name__, e)

df2 = pd.DataFrame({"close": range(100)})
try:
    df2.ta.sma(close=df2["close"], length=50)
    print("df2.ta.sma with Series worked")
except Exception as e:
    print("Error 2:", type(e).__name__, e)
