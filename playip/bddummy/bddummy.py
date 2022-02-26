from typing import Optional
from typing import List
from dynaconf import settings
from fastapi import APIRouter
from pydantic import Field

import pydantic
import fastapi


print("FASTAPI version ",fastapi.__version__)

bdbasicrouter = APIRouter(prefix="/playipispbd/basic")

class Endereco(pydantic.BaseModel):
    logradouro: str
    numero: str
    complemento: str
    cep: str
    condominio: Optional[str]
    cidade: str


class ContractData(pydantic.BaseModel):
    id_contract: str
    found: bool = False
    download_speed: Optional[int] = None
    upload_speed: Optional[int] = None
    is_radio: Optional[bool] = None
    is_ftth: Optional[bool] = None
    pack_name: Optional[str] = None
    user_name: Optional[str] = None
    home_access_key: Optional[str] = None
    home_access_type: Optional[str] = None
    endereco: Optional[Endereco] = None


@bdbasicrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str) -> ContractData:

    #hak = "william1.am.ftth"
    hak = "ivone2.sr.ftth"
    #hak =  "gilvanete.sb.ftth"

    if id_contract == "13000":
        endereco: Endereco = Endereco(logradouro="Rua Etiopia", numero="33", complemento="", cep="06660070", cidade="Itapevi")
        return ContractData(found=True, id_contract=id_contract,download_speed=100,upload_speed=50, is_radio=False, is_ftth=True,
                            home_access_key=hak, home_access_type="smartolt",
                            pack_name="PackDefault", endereco=endereco)
    else:
        return ContractData(id_contract=id_contract, found=False)


