from typing import Optional
from typing import List
from dynaconf import settings
from fastapi import APIRouter
from pydantic import Field

import pydantic
import fastapi

from playipappcommons.infra.endereco import Endereco
from playipappcommons.ispbd.ispbddata import Client, ContractData

print("FASTAPI version ",fastapi.__version__)

bdbasicrouter = APIRouter(prefix="/playipispbd/basic")




@bdbasicrouter.get("/getclientfromcpfcnpj/{cpfcnpj}", response_model=Client)
async def getClientFromCPFCNPJ(cpfcnpj:str) -> Client:
    if cpfcnpj=="07983764499":
        return Client(found=True, id_client="15594", name="Ivone", cpfcnpj=cpfcnpj)

@bdbasicrouter.get("/getcontracts/{id_client}", response_model=List[ContractData])
async def getContracts(id_client: str) -> List[ContractData]:
    contract_list: List[str] = []
    if id_client == "15594":
        contract_list.append("13000")
    contracts: List[ContractData] = []
    for c in contract_list:
        contract: ContractData = await getContract(c)
        contracts.append(contract)
    return contracts

@bdbasicrouter.get("/getcontractsfromcpfcnpj/{cpfcnpj}", response_model=List[ContractData])
async def getContractsFromCPFCNPJ(cpfcnpj: str) -> List[ContractData]:
    client: Client = await getClientFromCPFCNPJ(cpfcnpj)
    if client:
        contracts: List[ContractData] = await getContracts(client.id_client)
        return contracts
    else:
        return []


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


