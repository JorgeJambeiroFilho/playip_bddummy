from typing import Optional
from typing import List
from dynaconf import settings
from fastapi import APIRouter
from pydantic import Field

import pydantic
import fastapi


print("FASTAPI version ",fastapi.__version__)

bdbasicrouter = APIRouter(prefix="/playipispbd/basic")


class ContractData(pydantic.BaseModel):
    id_contract: str = 0
    download_speed: int = 0
    upload_speed:int = 0
    found:bool

@bdbasicrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str) -> ContractData:
    if id_contract == 13000:
        return ContractData(id_contract=id_contract,download_speed=100,upload_speed=50, found=True)
    else:
        return ContractData(id_contract=id_contract, found=True)


