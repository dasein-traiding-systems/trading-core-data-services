from fastapi import HTTPException

from services.backend import oracle
from services.backend.models import ErrorType


def check_is_oracle_initialized():
    if not oracle.initialized:
        raise HTTPException(status_code=400, detail={"message": "Oracle is not initialized",
                                                     "errorType": ErrorType.ORACLE_INIT_ERR.value})