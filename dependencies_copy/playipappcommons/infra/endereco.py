from typing import Optional

import pydantic

address_level_fields = [ "root", "prefix", "uf", "cidade", "bairro", "logradouro", "numero", "complemento"]

def increase_address_level(level:int):
    if level == -1:
        level = len(address_level_fields) - 1
    if level == 0:
        return None
    return level-1

def getFieldNameByLevel(level:int):
    if level ==-1:
        level = len(address_level_fields) - 1
    return address_level_fields[level]

# address_levels_up = {
#     "root": None,
#     "uf": "root",
#     "cidade": "uf",
#     "bairro": "cidade",
#     "logradouro": "bairro",
#     None: "logradouro"
# }



class Endereco(pydantic.BaseModel):
    logradouro: Optional[str]
    numero: Optional[str]
    complemento: Optional[str]
    bairro: Optional[str]
    cep: Optional[str]
    condominio: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    prefix: Optional[str]

    def setFieldValueByLevel(self, level:int, value:str):
        fn: str = getFieldNameByLevel(level)
        setattr(self, fn, value)
    def getFieldValueByLevel(self, level:int):
        fn:str = getFieldNameByLevel(level)
        v = getattr(self, fn)
        if v is None:
            v = ""
        return v

    def __repr__(self):
        return self.logradouro + ", " + self.numero + \
               (", " + self.complemento if self.complemento else "") + \
               (", " + self.condominio if self.condominio else "") + \
               (", " + self.bairro if self.bairro else "") + ". " \
               + self.cidade + "-" + self.uf + ". " + \
               ("CEP " + self.cep + "." if self.cep else "") + \
               ("Prefix" + self.prefix + "." if self.prefix else "")


def buildFullImportName(endereco: Endereco, nivel: int = -1):
    if nivel == 0:
        return ""
    upname = buildFullImportName(endereco, increase_address_level(nivel))
    cname = endereco.getFieldValueByLevel(nivel)
    cname = cname.replace("/","-")
    return upname+"/"+cname
