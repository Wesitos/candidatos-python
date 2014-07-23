import requests as req
import threading
from filtro import Filtro
import json
import time

lock_print = threading.Lock()
base_url = "http://200.48.102.67/pecaoe/servicios/"
dic_urls = {
    "principal": (base_url + "declaracion.asmx/" +
                  "CandidatoListarPorID"),
    "familia": (base_url + "declaracion.asmx/" +
                "CandidatoFamiliaListarPorCandidato"),
    "otraExperiencia": (base_url + "declaracion.asmx/" +
                        "CandidatoAdicionalListarPorCandidato"),
    "observaciones": (base_url + "simulador.asmx/" +
                      "Soporte_CandidatoAnotMarginal"),
    "ingresos": (base_url + "declaracion.asmx/" +
                 "IngresoListarPorCandidato"),
    "experiencia": (base_url + "declaracion.asmx/" +
                    "CandidatoExperienciaListarPorCandidato"),
    "educacionSuperior": (base_url + "declaracion.asmx/" +
                          "EducacionSuperiorListarPorCandidato"),
    "educacionBasica": (base_url + "declaracion.asmx/" +
                        "EducacionBasicaListarPorCandidato"),
    "militancia": (base_url + "declaracion.asmx/" +
                   "RenunciasOPListarPorCandidato"),
    "eleccion": (base_url + "declaracion.asmx/" +
                 "CargoEleccionListarPorCandidato"),
    "partidario": (base_url + "declaracion.asmx/" +
                   "CargoPartidarioListarPorCandidato"),
    "bienes": (base_url + "declaracion.asmx/" +
               "BienesListarPorCandidato"),
    "penal": (base_url + "declaracion.asmx/" +
              "AmbitoPenalListarPorCandidato"),
    "civil": (base_url + "declaracion.asmx/" +
              "AmbitoCivilListarPorCandidato"),
    "acreencias": (base_url + "declaracion.asmx/" +
                   "EgresosListarPorCandidato"),
}

headers = {"Content-Type": "application/json; charset=UTF-8",
           "Accept": "application/json"}

def imprime(cad):
    with lock_print:
        print cad

def genera_mensaje(id_Candidato):
    return {
        "objCandidatoBE": 
        {"objProcesoElectoralBE": {"intIdProceso": "72"},
                            "objOpInscritasBE": {"intCod_OP": "140"},
                            "intId_Candidato": str(id_Candidato)}
    }

def realiza_peticion(key,id_candidato, timeout=1):
    """Realiza una peticion, devuelve la respuesta (json) como
    un diccionario"""
    payload = genera_mensaje(id_candidato )
    url = dic_urls[key]
    kargs ={
        "data": json.dumps(payload),
        "headers": headers,
        "timeout": timeout}
    while True:
        try:
            r = req.post(url, **kargs)
        except req.Timeout:
            imprime("Timeout error")
            continue
        except req.ConnectionError as error:
            errno = error.errno
            err_msg  = "ConnectionError %d"%errno
            if errno == 101:
                err_msg += (": Esta conectado a internet?")
            imprime(err_msg)
            continue
        except Exception as e:
            imprime("Excepcion "+ str(e) )
            time.sleep(0.5)
            continue
        else:
            return r.json()

def get_data(key, id_cand):
    """Descarga y filtra data"""
    raw_data = realiza_peticion(key, id_cand)
    d_data = getattr(Filtro,"f_"+key)(raw_data)
    return d_data

def descarga_candidato(id_cand):
    """Descarga los datos y los filtra

    Si es un id valido, devuelve un diccionario con los datos.
    En caso contrario devuelve None"""
    dic_candidato = {"_id": id_cand}
    r_principal = get_data("principal", id_cand)
    # Verifica si el id es valido
    if not r_principal:
        dic_candidato["ok"] = False
        return dic_candidato
    else:
        dic_candidato["ok"] = True
        dic_candidato.update(r_principal)
    # ------------------------
    for key in dic_urls.keys():
        if key == "principal":
            continue
        dic_candidato[key] = get_data(key, id_cand)

    return dic_candidato
