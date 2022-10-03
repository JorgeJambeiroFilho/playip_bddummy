import asyncio
import time
import uuid
from typing import Optional, cast

from dynaconf import settings
from fastapi import APIRouter, Depends

from playipappcommons.auth.oauth2FastAPI import infrapermissiondep
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import ProcessAddressResult, importOrFindAddress, \
    addPrefixAndImportOrFindAddress, importAddressWithoutProcessing, ImportAddressResult, iar_key
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




async def getImportAddressResultIntern(mdb, begin:bool) -> ImportAddressResult:
    return cast(ProcessAddressResult, await getControlStructure(mdb, iar_key, begin))


@importrouter.get("/importaddresses", response_model=ImportAddressResult)
async def importAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingImportAddressResult: ImportAddressResult = await getImportAddressResultIntern(mdb, True)
    if onGoingImportAddressResult.hasJustStarted():
        asyncio.create_task(importAddressesIntern(mdb, onGoingImportAddressResult))
    return onGoingImportAddressResult

@importrouter.get("/getimportaddressesresult", response_model=ProcessAddressResult)
async def getImportAddressesResult(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    return await getImportAddressResultIntern(mdb, False)


async def importAddressesIntern(mdb, iar:ImportAddressResult):
    print("importAddressesIntern")
    iar.clearCounts()
    importExecUID: str = str(uuid.uuid1())
    time_ini = time.time()
    with open(settings.ADDRESSES_PATH+"/enderecos3.txt") as fp:

        lin = fp.readline()
        print(lin)
        lin = lin.strip()[1:-1]
        headers = [s.strip()[1:-1] for s in lin.split(",")]

        p = 0
        lin = fp.readline()
        while lin:


            #print(lin)
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
            else:
                print("Linha falha: ",lin)

            if await iar.saveSoftly(mdb):
                return


            lin = fp.readline()
            p += 1
    time_end = time.time()
    iar.done()
    await iar.saveHardly(mdb)
    print("Tempo de importação ", time_end - time_ini)
    print(iar)


@importrouter.get("/stopimportddresses", response_model=ImportAddressResult)
async def stopImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingIar: ImportAddressResult = await getImportAddressResultIntern(mdb, False)
    onGoingIar.abort()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar


@importrouter.get("/clearimportaddresses", response_model=ImportAddressResult)
async def clearImportAddresses(auth=Depends(infrapermissiondep)) -> ImportAddressResult:
    mdb = getBotMongoDB()
    onGoingIar = ImportAddressResult()
    await onGoingIar.saveSoftly(mdb)
    return onGoingIar
