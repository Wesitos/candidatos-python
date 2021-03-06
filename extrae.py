# -*- coding: iso-8859-15 -*-
# Aca se definen las funciones necesarias para descargar y filtrar
# la data
import threading
from filtro import Filtro
import json
import time
import requests as req
# Intenta importar requesocks
try:
    import requesocks as req_socks
except ImportError as err:
    _tor_available = False
    _tor_error = err
else:
    _tor_available = True

_tor_proxies = {'http': 'socks5://127.0.0.1:9050',
                'https': 'socks5://127.0.0.1:9050'}

lock_print = threading.Lock()
_base_url = "http://aplicaciones013.jne.gob.pe/pecaoe/servicios/"
_dic_urls = {
    "principal": (_base_url + "declaracion.asmx/" +
                  "CandidatoListarPorID"),
    "familia": (_base_url + "declaracion.asmx/" +
                "CandidatoFamiliaListarPorCandidato"),
    "otraExperiencia": (_base_url + "declaracion.asmx/" +
                        "CandidatoAdicionalListarPorCandidato"),
    "observaciones": (_base_url + "simulador.asmx/" +
                      "Soporte_CandidatoAnotMarginal"),
    "ingresos": (_base_url + "declaracion.asmx/" +
                 "IngresoListarPorCandidato"),
    "experiencia": (_base_url + "declaracion.asmx/" +
                    "CandidatoExperienciaListarPorCandidato"),
    "educacionSuperior": (_base_url + "declaracion.asmx/" +
                          "EducacionSuperiorListarPorCandidato"),
    "educacionBasica": (_base_url + "declaracion.asmx/" +
                        "EducacionBasicaListarPorCandidato"),
    "militancia": (_base_url + "declaracion.asmx/" +
                   "RenunciasOPListarPorCandidato"),
    "eleccion": (_base_url + "declaracion.asmx/" +
                 "CargoEleccionListarPorCandidato"),
    "partidario": (_base_url + "declaracion.asmx/" +
                   "CargoPartidarioListarPorCandidato"),
    "bienes": (_base_url + "declaracion.asmx/" +
               "BienesListarPorCandidato"),
    "penal": (_base_url + "declaracion.asmx/" +
              "AmbitoPenalListarPorCandidato"),
    "civil": (_base_url + "declaracion.asmx/" +
              "AmbitoCivilListarPorCandidato"),
    "acreencias": (_base_url + "declaracion.asmx/" +
                   "EgresosListarPorCandidato"),
}

_headers = {"Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json",
            # User-Agent falso, solo por si las dudas
            "User-Agent": ("Mozilla/5.0 (Windows NT 6.1; WOW64) " +
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" +
                           "36.0.1985.67 Safari/537.36")}


def imprime(cad):
    with lock_print:
        print cad


def genera_mensaje(id_Candidato):
    return {
        "objCandidatoBE":
        {"objProcesoElectoralBE": {"intIdProceso": "72"},
         "objOpInscritasBE": {"intCod_OP": "140"},
         "intEstado": 1,
         "intId_Candidato": str(id_Candidato)}
    }


def realiza_peticion(key, id_candidato, tor=False, timeout=2):
    """Realiza una peticion a una de las apis. Devuelve la
    respuesta (json) como un diccionario"""
    payload = genera_mensaje(id_candidato)
    url = _dic_urls[key]
    kargs = {
        "data": json.dumps(payload),
        "headers": _headers,
        "timeout": timeout}
    while True:
        try:
            if tor is True:
                tor_req = req_socks.session()
                tor_req.proxies = _tor_proxies
                r = tor_req.post(url, **kargs)
            else:
                r = req.post(url, **kargs)
        except req.exceptions.Timeout:
            imprime("Timeout error")
            continue
        except req.exceptions.ConnectionError as error:
            errno = error.errno
            err_msg = "ConnectionError"
            if errno == 101:
                err_msg += (": Esta conectado a internet?")
            imprime(err_msg)
            time.sleep(0.5)
            continue
        except Exception as e:
            imprime("Excepcion: " + str(e))
            time.sleep(0.5)
            continue
        else:
            if r.text.find("Attack Detected") != -1:
                imprime(r.text)
                imprime("Ataque detectado!! A dormir")
                time.sleep(60)
                continue
            if hasattr(r, 'json'):
                return r.json()
            else:
                return json.loads(r.content)


def filtra_data(key, raw_data):
    """Filtra uno de los diccionarios devueltos por las apis"""
    data = getattr(Filtro, "f_" + key)(raw_data)
    return data


def descarga_candidato(id_cand, filtrar=True, tor=False):
    """Descarga los datos de un candidato.

    "filtrar" indica si se devuelve un diccionario con la data
    filtrada o sin filtrar
    si es un id invalido, devuelve un diccionario con solo los
    campos de "_id" y "ok" """
    dic_candidato = {"_id": id_cand}
    raw_principal = realiza_peticion("principal", id_cand, tor)
    data_principal = filtra_data("principal", raw_principal)
    # Verifica si el id es valido
    if not data_principal:
        dic_candidato["ok"] = False
        return dic_candidato
    else:
        dic_candidato["ok"] = True
    # Verifica si se pudo importar requesocks. En caso contrario,
    # manda una excepcion
    if tor and (not _tor_available):
        raise _tor_error

    if filtrar:
        dic_candidato.update(data_principal)
        for key in _dic_urls.keys():
            if key == "principal":
                continue
            raw_data = realiza_peticion(key, id_cand, tor)
            dic_candidato[key] = filtra_data(key, raw_data)

    else:
        dic_candidato["principal"] = raw_principal
        for key in _dic_urls.keys():
            if key == "principal":
                continue
            dic_candidato[key] = realiza_peticion(key, id_cand, tor)

    return dic_candidato

def descarga_campo(id_cand, key, filtrar=True, tor=False):
    """Descarga un campo de un candidato"""
    dic_candidato = {"_id": id_cand}
    raw_data = realiza_peticion(key, id_cand, tor)


    if filtrar:
        dic_data = filtra_data(key, raw_data)
        dic_candidato[key] = dic_data
    else:
        dic_candidato[key] = raw_data

    return dic_candidato
