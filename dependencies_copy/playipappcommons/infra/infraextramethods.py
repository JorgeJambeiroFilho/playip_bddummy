from typing import List

from playipappcommons.famongo import FAMongoId
from playipappcommons.infra.endereco import getFieldNameByLevel, Endereco, buildFullImportName
from playipappcommons.infra.infraimportmethods import getInfraElementByFullImportName, importOrFindAddress
from playipappcommons.infra.inframethods import getInfraElementAddressHier, getInfraElement, isApproxAddr
from playipappcommons.infra.inframodels import InfraElement
from playipappcommons.ispbd.ispbddata import ContractData
from playipappcommons.playipchatmongo import getBotMongoDB
from playipappcommons.util.levenshtein import levenshteinDistanceDP






async def isApproxAddr2(enderecoCand:Endereco, enderecoCadastro:Endereco):
    fullName = buildFullImportName(enderecoCadastro)
    if not fullName:
        return False
    mdb = getBotMongoDB()
    #infraElement: InfraElement = await getInfraElementByFullImportName(mdb, fullName)
    infraElement: InfraElement = await importOrFindAddress(mdb, importResult=None, importExecUID=None, endereco=enderecoCadastro, doImport= False)
    if not infraElement:
        return False
    return await isApproxAddr(enderecoCand, infraElement.id, 0.5)


async def selectCompatibleContracts(enderecoCand:Endereco, contratos:List[ContractData]):
    selected: List[ContractData] = []
    for contrato in contratos:
        contrato.endereco.prefix = "Comercial"
        if await isApproxAddr2(enderecoCand, contrato.endereco):
            selected.append(contrato)
    return selected
