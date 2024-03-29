import asyncio
import traceback
from datetime import datetime
from dateutil import tz
from dateutil.tz import tzutc
from typing import Iterable, cast

from dynaconf import settings
from fastapi import APIRouter

from playipappcommons.analytics.contractsanalytics import ContractAnalyticData, ServicePackAnalyticData, \
    ServicePackAndContractAnalyticData, ImportContractsResult
from playipappcommons.analytics.contractsanalyticsmodels import TicketData, iadr_key
from playipappcommons.analytics.contractsimport import import_contracts_raw
from playipappcommons.basictaskcontrolstructure import getControlStructure
from playipappcommons.infra.endereco import Endereco



#onGoingImportAnalyticDataResult: ImportAnalyticDataResult = None


def cf(s):
    r = s.strip().strip("'").strip().replace("/","-")
    if r == "None":
        r = None
    return r



class ObjRow:
    pass

#tzsp = tz.timezone('America/Sao_Paulo')
tzsp = tz.tzoffset('IST', -10800)


async def getContratoPacoteServicoTicketIterator():


    with open(settings.ANALYTICS_PATH+"/tickets5.txt") as fp:
        p = 0
        lin = fp.readline()
        lin = lin.strip()[1:-1]
        headers = [s.strip()[1:-1] for s in lin.split(",")]
        lin = fp.readline()
        while lin:
            lin = lin.strip()[1:-1]
            lis = lin.split(',')
            lis = [s.strip() for s in lis]
            row = ObjRow()
            for h, v in zip(headers, lis):
                v = cf(v)
                if v and (h.startswith("DT") or "_DT_" in h or h.endswith("_DT")):
                    try:
                        dtlis = v.split("-") #[1:-1]
                        v = datetime(year=int(dtlis[0]), month=int(dtlis[1]), day=int(dtlis[2]), tzinfo=tzutc())
                        vbr = datetime(year=int(dtlis[0]), month=int(dtlis[1]), day=int(dtlis[2]), tzinfo=tzsp)
                        v = v.timestamp()
                        vbr = vbr.timestamp()
                    except Exception as e:
                        raise
                #v = v.strip()
                # if v == "None":
                #     v = None
                # elif len(v) >= 2 and v[0]=="'" and v[-1]=="'":
                #     v = v[1:-1].strip()
                row.__setattr__(h,v)

            is_radio = row.NM_MEIO == "radio"  # a outra opção no wgc é "fibra"
            medianetwork = "Rádio" if is_radio else "Cabo"
            enderecoInfra: Endereco = Endereco\
            (
                    logradouro=row.logradouro, numero=row.num, complemento=row.complemento, bairro=row.bairro, cep=row.cep,
                    condominio=row.condominio, cidade=row.cidade, uf=row.id_uf, prefix="Infraestrutura-"+medianetwork
            )
            enderecoComercial: Endereco = Endereco\
            (
                    logradouro=row.logradouro, numero=row.num, complemento=row.complemento, bairro=row.bairro, cep=row.cep,
                    condominio=row.condominio, cidade=row.cidade, uf=row.id_uf, prefix="Comercial"
            )
            #for endereco in [enderecoInfra, enderecoComercial]:
            contract: ContractAnalyticData = ContractAnalyticData\
            (
                id_contract=row.ID_CONTRATO, #+("_infra" if endereco is enderecoInfra else "_comercial"),
                DT_ATIVACAO=row.CONTRATO_DT_ATIVACAO,
                DT_CANCELAMENTO=row.CONTRATO_DT_CANCELAMENTO,
                DT_INICIO=row.CONTRATO_DT_INICIO,
                DT_FIM=row.CONTRATO_DT_FIM,
                STATUS_CONTRATO=row.STATUS_CONTRATO,
                enderecos=[enderecoComercial, enderecoInfra] #enderecoInfra, só vou contar esses eventos na linha comercial, os eventos que interessam paar estrutura são outros.
            )
            service: ServicePackAnalyticData = ServicePackAnalyticData\
            (
                fullName = row.NM_PROD + "/" + row.NM_MEIO + "/" + row.NM_TEC + "/" + row.NM_PACOTE_SERVICO, #+ "/", # a última barra garante que todos os prefixos relevantes terminem em "/". Isso por sua vez evita problemas que apareceriam se um nome em um nível fosse prefixo de outro
                DT_ATIVACAO=row.SERVICO_DT_ATIVACAO,
                DT_DESATIVACAO=row.SERVICO_DT_DESATIVACAO,
                DT_DESISTENCIA=row.SERVICO_DT_DESISTENCIA,
                DT_CADASTRO=row.SERVICO_DT_CADASTRO,
                TX_MOTIVO_CANCELAMENTO=row.SERVICO_TX_MOTIVO_CANCELAMENTO if row.SERVICO_TX_MOTIVO_CANCELAMENTO else "Desconhecido",
                VL_SERVICO=row.VL_PACOTE, # só há um serviço, relevante,então posso jogar o preço do pacote todod nele para fins estatísticos
                download_speed=row.VL_DOWNLOAD,
                upload_speed=row.VL_UPLOAD,
                VL_PACOTE=row.VL_PACOTE
            )
            ticket: TicketData = TicketData\
            (
                DT_ABERTURA=row.DT_ticketAbertura,
                DT_FECHAMENTO=row.DT_ticketFechamento,
                NM_AREA_TICKET=row.ticketArea
            )
            spc: ServicePackAndContractAnalyticData = ServicePackAndContractAnalyticData(contract=contract, service=service, ticket=ticket)
            #if enderecoComercial.bairro == "Vila Santa Rita":
            yield spc


            lin = fp.readline()


async def getImportContractsResultIntern(mdb, begin:bool) -> ImportContractsResult:
    return cast(ImportContractsResult, await getControlStructure(mdb, iadr_key, begin))


async def importAllContratoPacoteServicoTicket(mdb, iadr:ImportContractsResult):
    it: Iterable[ServicePackAndContractAnalyticData] = getContratoPacoteServicoTicketIterator()
    await import_contracts_raw(it, iadr)
