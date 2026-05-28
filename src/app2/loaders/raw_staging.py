from sqlalchemy import Integer, JSON, Text, bindparam, text
from sqlalchemy.engine import Engine

_INSERT_QUERY = (
    text(
        """
        INSERT INTO stg.raw_football_api (endpoint, request_params, http_status, response_json, batch_id)
        VALUES (:endpoint, :request_params, :http_status, :response_json, :batch_id)
        """
    )
    .bindparams(
        bindparam("endpoint", type_=Text),
        bindparam("request_params", type_=JSON),
        bindparam("http_status", type_=Integer),
        bindparam("response_json", type_=JSON),
        bindparam("batch_id", type_=Text),
    )
)


def load_raw(engine: Engine, endpoint: str, status_code: int, payload, metadata: dict | None = None):
    params = metadata or {}
    batch_id = params.get("batch_id") if params else None
    with engine.begin() as conn:
        result = conn.execute(
            _INSERT_QUERY,
            {
                "endpoint": endpoint,
                "request_params": params,
                "http_status": status_code,
                "response_json": payload,
                "batch_id": batch_id,
            },
        )
    return int(result.rowcount or 0)