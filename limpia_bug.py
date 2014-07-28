#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
# Para limpiar la data sucia generada por el bug de descarga_candidatos
from filtro import Filtro
from pymongo import MongoClient

_client = None
_database = "candidatos"

def conectar_db(host="localhost", port=27017,
                db=None):
    """Crea un cliente de la base de datos Mongodb"""
    global _client 
    _client = MongoClient(host, port)
    if db:
        global _database
        _database = db

def limpia_colleccion(origen = "candFiltrado", destino="candLimpio"):
    """Limpia la colleccion especificada por 'origen' sin alterarla
    los datos limpios los almacena en la coleccion especificada por
    'destino'"""
    db = _client[_database]
    collect_origen = db[origen]
    collect_dest = db[destino]
    datos_origen = collect_origen.find()
    for data_sucia in datos_origen:
        data_limpia = Filtro.f_data_sucia(data_sucia)
        collect_dest.insert(data_limpia)
        print data_limpia["_id"]
