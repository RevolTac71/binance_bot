import pandas as pd
import pandas_ta as ta

df = pd.DataFrame({"close": range(200)})
try:
    df.ta.ema(length=50, append=True, col_names=("EMA_50",))
    print("ta.ema tuple worked")
except Exception as e:
    import traceback

    traceback.print_exc()
