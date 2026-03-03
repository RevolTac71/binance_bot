import pandas as pd

try:
    s = pd.Series(["1.83711"])
    print(s.astype("float32"))
    print("SUCCESS")
except Exception as e:
    print("ERROR:", e)
