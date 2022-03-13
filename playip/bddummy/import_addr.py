import asyncio
import time
import uuid
from typing import Optional

from dynaconf import settings
from fastapi import APIRouter

from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ImportAddressResult, importOrFindAddress
from playipappcommons.playipchatmongo import getBotMongoDB

importrouter = APIRouter(prefix="/playipispbd/import")

sq = """
    SELECT  
            Endereco.TX_ENDERECO as logradouro, Endereco.NR_NUMERO as num, Endereco.TX_COMPLEMENTO as complemento, 
            Endereco.TX_CEP as cep, Condominio.NM_CONDOMINIO as condominio, Endereco.TX_BAIRRO as bairro, Cidade.ID_LOCALIDADE as id_cidade, 
            Cidade.TX_NOME_LOCALIDADE as cidade,UF.ID_UF as id_uf, UF.NM_UF as uf        
    FROM 
        Endereco as Endereco
            LEFT JOIN LOG_LOCALIDADE as Cidade on (Endereco.ID_CIDADE=Cidade.ID_LOCALIDADE)
            LEFT JOIN Condominio as Condominio on (Endereco.ID_CONDOMINIO=Condominio.ID_CONDOMINIO)
            LEFT JOIN LOG_UF as UF on (Cidade.ID_UF_LOCALIDADE=UF.ID_UF)
    WHERE 
            Endereco.ID_ENDERECO > {param_last_id_endereco_imported}
    ORDER BY 
            UF.ID_UF, Cidade.ID_LOCALIDADE, Endereco.TX_BAIRRO, Endereco.TX_ENDERECO
                 """#.format(param_last_id_endereco_imported=last_id_endereco_imported))

def cf(s):
    r = s.strip().strip("'").strip()
    if r == "None":
        r = None
    return r


onGoingImportAddressResult: ImportAddressResult = None

@importrouter.get("/importaddresses", response_model=ImportAddressResult)
async def importAddresses() -> ImportAddressResult:
    global onGoingImportAddressResult
    if onGoingImportAddressResult is None or onGoingImportAddressResult.complete:
        onGoingImportAddressResult = ImportAddressResult()
        asyncio.create_task(importAddressesIntern())
    return onGoingImportAddressResult

@importrouter.get("/getimportaddressesresult", response_model=ImportAddressResult)
async def getImportAddressesResult() -> ImportAddressResult:
    global onGoingImportAddressResult
    if onGoingImportAddressResult is None:
        onGoingImportAddressResult = ImportAddressResult()
        onGoingImportAddressResult.complete = True
    return onGoingImportAddressResult


async def importAddressesIntern() -> ImportAddressResult:
    global onGoingImportAddressResult
    if onGoingImportAddressResult is None or onGoingImportAddressResult.complete:
        onGoingImportAddressResult = ImportAddressResult()

    mdb = getBotMongoDB()
    #global onGoingImportAddressResult
    res: ImportAddressResult = onGoingImportAddressResult
    importExecUID: str = str(uuid.uuid1())
    time_ini = time.time()
    with open(settings.ADDRESSES_PATH+"/enderecos.txt") as fp:
        p = 0
        lin = fp.readline()
        while lin:
            lin = lin[1:-2]
            #print("|"+lin+"|")

            row = lin.split(",")
            if len(row)==10:
                #row = [s.strip().strip("'").strip() for s in row]
                #print (row)

                logradouro: Optional[str] = cf(row[0])
                numero: Optional[str] = cf(row[1])
                complemento: Optional[str] = cf(row[2])
                bairro: Optional[str] = cf(row[5])
                cep: Optional[str] = cf(row[3])
                condominio: Optional[str] = cf(row[4])
                cidade: Optional[str] = cf(row[7])
                uf: Optional[str] = cf(row[8])

                endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, medianetwork="Mixed")
                #print(endereco)
                #if cidade is not None and cidade.lower() == cidade_alvo.lower():
                await importOrFindAddress(mdb, res, importExecUID, endereco)
                res.num_processed += 1

            lin = fp.readline()
            p += 1
    time_end = time.time()
    res.complete = True
    print("Tempo de importação ", time_end - time_ini)
    print(res)
    return res

