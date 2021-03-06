import asyncio
import traceback
from datetime import datetime
from typing import Iterable

from dynaconf import settings
from fastapi import APIRouter

from playipappcommons.analytics.analytics import ContractAnalyticData, ServicePackAnalyticData, \
    ServicePackAndContractAnalyticData, count_events_contracts_raw, ImportAnalyticDataResult, \
    getImportAnalyticDataResult, setImportAnalyticDataResult
from playipappcommons.infra.endereco import Endereco


#onGoingImportAnalyticDataResult: ImportAnalyticDataResult = None

def cf(s):
    r = s.strip().strip("'").strip().replace("/","-")
    if r == "None":
        r = None
    return r



class ObjRow:
    pass

async def getContratoPacoteServicoIterator():


    with open(settings.ANALYTICS_PATH+"/contrato_pacote_servico_list.txt") as fp:
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
                        v = datetime(year=int(dtlis[0]), month=int(dtlis[1]), day=int(dtlis[2]))
                        v = v.timestamp()
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
                enderecos=[enderecoComercial] #enderecoInfra, só vou contar esses eventos na linha comercial, os eventos que interessam paar estrutura são outros.
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
            spc: ServicePackAndContractAnalyticData = ServicePackAndContractAnalyticData(contract=contract, service=service)
            yield spc
            lin = fp.readline()


async def importAllContratoPacoteServico():
    onGoingImportAnalyticDataResult = await getImportAnalyticDataResult(True)
    if onGoingImportAnalyticDataResult.started:
        return
    onGoingImportAnalyticDataResult.started = True
    await setImportAnalyticDataResult(onGoingImportAnalyticDataResult)

    it: Iterable[ServicePackAndContractAnalyticData] = getContratoPacoteServicoIterator()
    await count_events_contracts_raw(it, onGoingImportAnalyticDataResult)
