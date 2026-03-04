import pandas as pd
import pandas_ta as ta

df = pd.DataFrame(
    {"high": range(10, 110), "low": range(0, 100), "close": range(5, 105)}
)
df.ta.adx(length=14, append=True, col_names=("ADX_14", "DMP_14", "DMN_14"))
print(df.columns)

try:
    df["ADX_SMA_50"] = df.ta.sma(close=df["ADX_14"], length=50)
    print("ta.sma using series worked")
except Exception as e:
    print("Error 1:", type(e).__name__, e)

try:
    df["ADX_SMA_50"] = df.ta.sma(close="ADX_14", length=50)
    print("ta.sma using string worked")
except Exception as e:
    print("Error 2:", type(e).__name__, e)

try:
    df["ADX_SMA_50"] = ta.sma(df["ADX_14"], length=50)
    print("ta.sma using explicit kwargs worked")
except Exception as e:
    print("Error 3:", type(e).__name__, e)
