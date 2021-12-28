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
    is_radio:bool = False
    is_ftth: bool = False
    found:bool = False
    pack_name:str = "PackDefault"
    home_access_key:Optional[str] = None
    home_access_type:Optional[str] = None


@bdbasicrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str) -> ContractData:

    #hak = "william1.am.ftth"
    #hak = "ivone2.sr.ftth"
    hak =  "gilvanete.sb.ftth"

    if id_contract == "13000":
        return ContractData(id_contract=id_contract,download_speed=100,upload_speed=50, is_radio=False, is_ftth=True,
                            home_access_key=hak, home_access_type="smartolt",
                            found=True, )
    else:
        return ContractData(id_contract=id_contract, found=False)


