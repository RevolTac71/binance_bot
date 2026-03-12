import numpy as np
import pandas as pd
import json

def clean_json_data(data):
    """
    전달받은 데이터를 JSON 직렬화 가능한 순수 파이썬 타입으로 재귀적으로 변환합니다.
    - Pydantic 모델: .model_dump() 호출
    - Numpy 타입: native 파이썬 타입으로 변환
    - NaN/NaT: None으로 변환
    - 재귀적으로 dict, list, tuple, set 처리
    """
    if isinstance(data, dict):
        return {k: clean_json_data(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple, set)):
        return [clean_json_data(i) for i in data]
    elif hasattr(data, "model_dump"):  # Pydantic 모델 대응
        return clean_json_data(data.model_dump())
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, (np.int64, np.int32, np.int16, np.int8)):
        return int(data)
    elif isinstance(data, (np.float64, np.float32, np.float16)):
        return float(data)
    elif pd.isna(data):
        return None
    return data
