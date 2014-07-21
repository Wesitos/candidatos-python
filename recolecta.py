#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

import threading
import requests as req
import json
import Queue
        
def Recolector(threading.Thread, Filtro):
    """Realiza las peticiones """
    headers = { "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",}
    dic_urls ={
        "principal": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CandidatoListarPorID",
        "familia": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CandidatoFamiliaListarPorCandidato",
        "otra_experiencia": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CandidatoAdicionalListarPorCandidato",
        "observaciones": "http://200.48.102.67/pecaoe/servicios/simulador.asmx/Soporte_CandidatoAnotMarginal",
        "ingresos": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/IngresoListarPorCandidato",
        "experiencia": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CandidatoExperienciaListarPorCandidato",
        "educacion_superior": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/EducacionSuperiorListarPorCandidato",
        "educacion_basica": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/EducacionBasicaListarPorCandidato",
        "militancia": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/RenunciasOPListarPorCandidato",
        "eleccion": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CargoEleccionListarPorCandidato",
        "partidario": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/CargoPartidarioListarPorCandidato",
        "bienes": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/BienesListarPorCandidato",
        "penal": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/AmbitoPenalListarPorCandidato",
        "civil": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/AmbitoCivilListarPorCandidato",
        "acreencias": "http://200.48.102.67/pecaoe/servicios/declaracion.asmx/EgresosListarPorCandidato",
    }
    def __init__(self):
        pass
        
    def genera_payload(self,id_Candidato):
        payload = {
            "objCandidatoBE": {
                "objProcesoElectoralBE":{
                    "intIdProceso": "72"},
                "objOpInscritasBE":{
                    "intCod_OP":"140"},
                "intId_Candidato": str(id_Candidato)}
        }
        
    def realiza_peticion(self, key, id_candidato):
        """Realiza una peticion, devuelve la respuesta (json) como
        un diccionario

        Nos aprovechamos del hecho de que todas las peticiones tienen
        el mismo contenido"""
        cont = self.genera_payload(id_candidato)
        url = self.dic_urls[key]
        r = req.post(url, data = json.dumps(cont), headers = self.headers)
        return r.json()
    
    @staticmethod
    def put_task(dic_datos):
        pass

