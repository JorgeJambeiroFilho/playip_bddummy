import asyncio
import time
import uuid
from typing import Optional

from dynaconf import settings
from fastapi import APIRouter, Depends

from playipappcommons.auth.oauth2FastAPI import infrapermissiondep
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ProcessAddressResult, importOrFindAddress, \
    addPrefixAndImportOrFindAddress, importAddressWithoutProcessing, ImportAddressResult
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
    r = s.strip().strip("'").strip().replace("/","-")
    if r == "None":
        r = None
    return r


#onGoingImportAddressResult: ProcessAddressResult = None



async def getImportAddressResultIntern(mdb, begin:bool) -> ImportAddressResult:
    resDict = await mdb.control.find_one({"key":"ImportAddresses"})
    if resDict is None:
        res = ImportAddressResult()
        res.complete = True
    else:
        res: ImportAddressResult = ImportAddressResult(**resDict)
    if res.complete and begin:
        # Recomeça, pois não há um controle que permita continuar
        # Pode recomeçar porque as operações são idempotentes
        res = ImportAddressResult()
        await setImportAddressResult(mdb, res)
    return res

async def setImportAddressResult(mdb, iar:ImportAddressResult):
    resDict = iar.dict(by_alias=True)
    resDict["key"] = "ImportAddresses"
    iar.last_action = time.time()
    await mdb.control.replace_one({"key": "ImportAddresses"}, resDict, upsert=True)

@importrouter.get("/importaddresses", response_model=ImportAddressResult)
async def importAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingImportAddressResult: ImportAddressResult = await getImportAddressResultIntern(mdb, True)
    if not onGoingImportAddressResult.started:
        asyncio.create_task(importAddressesIntern())
    return onGoingImportAddressResult

@importrouter.get("/getimportaddressesresult", response_model=ProcessAddressResult)
async def getImportAddressesResult(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    return await getImportAddressResultIntern(mdb, False)


async def importAddressesIntern():
    print("importAddressesIntern")
    mdb = getBotMongoDB()
    iar: ImportAddressResult = await getImportAddressResultIntern(mdb, True)
    importExecUID: str = str(uuid.uuid1())
    time_ini = time.time()
    with open(settings.ADDRESSES_PATH+"/enderecos3.txt") as fp:

        iar2: ImportAddressResult = await getImportAddressResultIntern(mdb, False)
        if iar2.aborted:
            iar.complete = True
            iar.aborted = True
            await setImportAddressResult(mdb, iar)
            return

        lin = fp.readline()
        print(lin)
        lin = lin.strip()[1:-1]
        headers = [s.strip()[1:-1] for s in lin.split(",")]

        p = 0
        lin = fp.readline()
        while lin:
            print(lin)
            lin = lin[1:-2]
            #print("|"+lin+"|")

            row = lin.split(",")
            if len(row) == 11:
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
                is_radio = cf(row[10]) == "radio"  # a outra opção no wgc é "fibra"
                medianetwork = "Rádio" if is_radio else "Cabo"
                endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento,
                                              bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf)

                await importAddressWithoutProcessing(mdb, iar, importExecUID, endereco, medianetwork)

                # endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, prefix="Infraestrutura-"+medianetwork)
                # await importOrFindAddress(mdb, res, importExecUID, endereco)
                # res.num_processed += 1
                #
                # endereco: Endereco = Endereco(logradouro=logradouro, numero=numero, complemento=complemento, bairro=bairro, cep=cep, condominio=condominio, cidade=cidade, uf=uf, prefix="Comercial")
                # await importOrFindAddress(mdb, res, importExecUID, endereco)
                # res.num_processed += 1

            lin = fp.readline()
            p += 1
    time_end = time.time()
    iar.complete = True
    print("Tempo de importação ", time_end - time_ini)
    print(iar)


@importrouter.get("/stopimportddresses", response_model=ImportAddressResult)
async def stopImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingIar: ImportAddressResult = await getImportAddressResultIntern(mdb, False)
    if onGoingIar.started:
        onGoingIar.aborted = True
    mdb = getBotMongoDB()
    await setImportAddressResult(mdb, onGoingIar)
    return onGoingIar


@importrouter.get("/clearimportaddresses", response_model=ImportAddressResult)
async def clearImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingPar: ImportAddressResult = await getImportAddressResultIntern(mdb, False)
    if onGoingPar.started:
        onGoingPar.message = "CannotClearRunningProcess"
    else:
        # faz começar do zero, mas esse processo sempre volta para o zero quando para, lago essa operação
        # está aqui só para manter a analogia com outros semelhantes, mas que nem sempre recomeçam
        onGoingPar = ImportAddressResult()
        await setImportAddressResult(mdb, onGoingPar)
    return onGoingPar
