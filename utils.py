import numpy as np
import pandas as pd

def clean_json_data(data):
    """
    전달받은 데이터를 JSON 직렬화 가능한 순수 파이썬 타입으로 재귀적으로 변환합니다.
    - Pydantic 모델: .model_dump() 호출
    - Numpy 타입: .item() 또는 리스트로 변환 (Scalar/Array 대응)
    - Pandas 타입: ISO 포맷 문자열 또는 None으로 변환
    - 재귀적으로 dict, list, tuple, set 처리
    """
    if isinstance(data, dict):
        # JSONB 컬럼이나 json.dumps()에서 키가 문자열이 아닐 경우 에러가 발생할 수 있으므로 str() 처리
        return {str(k): clean_json_data(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple, set)):
        return [clean_json_data(i) for i in data]
    elif hasattr(data, "model_dump"):  # Pydantic 모델 대응
        return clean_json_data(data.model_dump())
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, (np.integer, np.floating)):
        return data.item()
    elif isinstance(data, np.ndarray):
        return clean_json_data(data.tolist())
    elif isinstance(data, pd.Timestamp):
        return data.isoformat()
    elif pd.isna(data):
        return None
    elif isinstance(data, (int, float, bool, str)) or data is None:
        return data
    else:
        # 알 수 없는 타입은 문자열로 변환하여 에러 방지 (Object of type XXX is not JSON serializable 방어)
        return str(data)
