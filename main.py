#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
from maestro import Maestro
from recolecta import Recolector
from Queue import Queue
from pymongo import MongoClient
import threading

db = MongoClient()
collection = "hojas-vida"
lock_output = threading.Lock()

def db_inserta(dato):
    collect = db[collection]
    item_id = collect.insert(dic_datos)
    with lock_output:
        print "Insertado id: %d"%item_id

# Generador de parametros
gen_params = (tuple() for x in range(5))

# Generador de tareas
gen_tasks = ((x,) for x in range(1, 17))

# Crea objeto maestro

master_params ={
    "n_threads": 5,
    "ClaseThread": Recolector,
    "done": db_inserta
    
}

master = Maestro(5, Recolector, gen_params, gen_tasks)

# Inicia
print "Iniciando"
master.iniciar()

print "Finalizado"
