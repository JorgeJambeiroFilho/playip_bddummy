def compara_ativos():

    ativosAppSt = {}
    ativosApp = set()
    ativosAddy = set()


    fp = open("contratos_nao_cancelados_2.txt")
    lin = fp.readline()
    while lin:
        #print(lin)
        lin = lin[1:-1]
        lis = lin.split(",")
        nc = int(lis[0])
        ativosApp.add(nc)
        ativosAppSt[nc] = lin
        lin = fp.readline()
    fp.close()

    fp = open("contratos_ativos_addy.txt")
    lin = fp.readline()
    while lin:
        #print(lin)
        nc = int(lin)
        ativosAddy.add(nc)
        lin = fp.readline()
    fp.close()

    setAddyNotApp = ativosAddy - ativosApp
    setAppNotAddy = ativosApp - ativosAddy


    print(len(ativosAddy))
    print(len(ativosApp))
    print(len(setAddyNotApp))
    print(len(setAppNotAddy))

    lisAppNotAddy = list(setAppNotAddy)
    lisAppNotAddy.sort()
    for lin in [ativosAppSt[nc] for nc in lisAppNotAddy]:
        print(lin)


if __name__ == "__main__":
    compara_ativos()