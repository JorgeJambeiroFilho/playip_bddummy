from typing import cast

import asyncio

from fastapi import APIRouter, Depends

from playip.bddummy.import_contracts_tickets import importAllContratoPacoteServicoTicket, getImportContractsResultIntern
from playipappcommons.analytics.analyticsmodels import ImportAnalyticDataResult, iadr_key, ProcessAnalyticDataResult
from playipappcommons.auth.oauth2FastAPI import analyticspermissiondep
from playipappcommons.playipchatmongo import getBotMongoDB

importanalyticsrouter = APIRouter(prefix="/playipispbd/importcontracts")

@importanalyticsrouter.get("/importcontractswithtickets", response_model=ImportAnalyticDataResult)
async def importContracts(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:

    mdb = getBotMongoDB()
    onGoingImportAnalyticsResult: ImportAnalyticDataResult = await getImportContractsResultIntern(mdb, True)
    if onGoingImportAnalyticsResult.hasJustStarted():
        asyncio.create_task(importAllContratoPacoteServicoTicket(mdb, onGoingImportAnalyticsResult))
    return onGoingImportAnalyticsResult



@importanalyticsrouter.get("/getimportcontractswithticketsresult", response_model=ImportAnalyticDataResult)
async def getImportContractsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
    mdb = getBotMongoDB()
    return await getImportContractsResultIntern(mdb, False)


# @importanalyticsrouter.get("/importanalytics", response_model=ImportAnalyticDataResult)
# async def importAnalytics(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
#     if not onGoingImportAnalyticDataResult.started:
#         asyncio.create_task(importAllContratoPacoteServico())
#     return onGoingImportAnalyticDataResult


# @importanalyticsrouter.get("/getimportanalyticsresult", response_model=ImportAnalyticDataResult)
# async def getImportAnalyticsResult(auth=Depends(analyticspermissiondep)) -> ImportAnalyticDataResult:
#     return await getImportAnalyticDataResult(False)

