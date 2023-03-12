import datetime
from datetime import date
from typing import Optional
from typing import List
from dynaconf import settings
from fastapi import APIRouter, Depends
from pydantic import Field

import pydantic
import fastapi

from playipappcommons.auth.oauth2FastAPI import defaultpermissiondep
from playipappcommons.infra.endereco import Endereco
from playipappcommons.ispbd.ispbddata import Client, ContractData

print("FASTAPI version ",fastapi.__version__)
print("Prints")

bdbasicrouter = APIRouter(prefix="/playipispbd/basic")




@bdbasicrouter.get("/getclientfromcpfcnpj/{cpfcnpj}", response_model=Client)
async def getClientFromCPFCNPJ(cpfcnpj:str, auth=Depends(defaultpermissiondep)) -> Client:
    print("get client ", cpfcnpj)
    if cpfcnpj=="07983764499":
        return Client(found=True, id_client="15594", name="Ivone", cpfcnpj=cpfcnpj)
    if cpfcnpj=="29875914894":
        return Client(found=True, id_client="7719", name="MTabian", cpfcnpj=cpfcnpj)

@bdbasicrouter.get("/getcontracts/{id_client}", response_model=List[ContractData])
async def getContracts(id_client: str, auth=Depends(defaultpermissiondep)) -> List[ContractData]:
    print("get contracts client ",id_client)
    contract_list: List[str] = []
    if id_client == "15594":
        contract_list.append("13000")
    if id_client == "7719":
        contract_list.append("19700")
        contract_list.append("16937")

    contracts: List[ContractData] = []
    for c in contract_list:
        contract: ContractData = await getContract(c)
        contracts.append(contract)
    return contracts

@bdbasicrouter.get("/getcontractsfromcpfcnpj/{cpfcnpj}", response_model=List[ContractData])
async def getContractsFromCPFCNPJ(cpfcnpj: str, auth=Depends(defaultpermissiondep)) -> List[ContractData]:
    print("get contracts cpfcnpj", cpfcnpj)
    client: Client = await getClientFromCPFCNPJ(cpfcnpj)
    if client:
        contracts: List[ContractData] = await getContracts(client.id_client)
        return contracts
    else:
        return []


@bdbasicrouter.get("/getcontract/{id_contract}", response_model=ContractData)
async def getContract(id_contract:str, auth=Depends(defaultpermissiondep)) -> ContractData:

    print("get contract ", id_contract)
    #hak = "william1.am.ftth"
    hak = "ivone2.sr.ftth"
    #hak =  "gilvanete.sb.ftth"
    if id_contract == "19700" or id_contract == "16937":
        print(19700)
        endereco: Endereco = Endereco(logradouro="Praça Alpha de Centauro (Centro de Apoio 2)", numero="20", complemento="sala7", bairro="Alphaville", cep="06541075", cidade="Santana de Parnaíba", uf="SP", prefix=None)
        return ContractData(found=True, id_contract=id_contract,download_speed=250,upload_speed=125, is_radio=False, is_ftth=True,
                            home_access_key=hak, home_access_type="smartolt",
                            pack_name="PackDefault", endereco=endereco, bloqueado=False,
                            dt_ativacao=datetime.datetime(year=2018, month=1, day=1).timestamp(), dt_cancelamento=None)

    elif id_contract == "13000":
        print(13000)
        endereco: Endereco = Endereco(logradouro="Rua Etiopia", numero="33", complemento="", bairro="Jardim Santa Rita", cep="06660070", cidade="Itapevi", uf="SP", prefix=None)
        return ContractData(found=True, id_contract=id_contract,download_speed=100,upload_speed=50, is_radio=False, is_ftth=True,
                            home_access_key=hak, home_access_type="smartolt",
                            pack_name="PackDefault", endereco=endereco, bloqueado=False,
                            dt_ativacao=datetime.datetime(year=2018, month=1, day=1).timestamp(), dt_cancelamento=None)
    else:
        return ContractData(id_contract=id_contract, found=False)


