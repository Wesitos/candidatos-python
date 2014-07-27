#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
# Aca se definen funciones para trabajar con multiples
# threads y Mongodb
from pymongo import MongoClient
from extrae import descarga_candidato, lock_print
import threading
import time

# Creamos el cliente de Mongodb solo una vez
# Por defecto se conecta a 'localhost' por el puerto 27017
_client = MongoClient()

# Donde guardaremos la informacion en la base de datos
_database = "candidatos"
_collection = "candFiltrado"


def db_inserta(dato, contador=[0]):
    """Inserta un dato en la collecion indicada

    La variable 'contador' se comporta como una variable estatica.
    Nos permite limitar la cantidad de mensajes que imprimimos en
    la terminal"""
    db = _client[_database]
    contador[0] += 1
    collect = db[_collection]
    item_id = collect.insert(dato)
    with lock_print:
        if not contador[0] % 20:
            print "Insertado id: %d" % item_id


# Esto es un plagio, pero funciona
# http://stackoverflow.com/questions/13456735/
# how-to-wrap-a-python-iterator-to-make-it-thread-safe
class LockedIterator(object):
    """Iterador que soporta multithreading"""
    def __init__(self, it):
        self._lock = threading.Lock()
        self._it = it.__iter__()
        if hasattr(self._it, 'close'):
            def close(self):
                with self._lock:
                    self._it.close()
            self.__setattr__('close', close)

    def __iter__(self):
        return self

    def next(self):
        with self._lock:
            return self._it.next()


def genera_target(iter_get_params, foo_do, foo_done):
    """Devuelve una funcion para ser llamada por los threads

    Los threads van a conseguir del iterador una lista de parametros
    con los cuales llamaran a la funcion 'foo_do'. Despues, con el valor
    de retorno de esta funcion llamaran a 'foo_done'"""
    safe_iterable = LockedIterator(iter_get_params)

    def target():
        for item in safe_iterable:
            res = foo_do(item)
            foo_done(res)
        with lock_print:
            print "Hilo terminado"

    return target


def genera_threads(n_threads, iter_get_params, foo_do, foo_done):
    """Construye y devuelve una lista de threads

    'n_threads' es el numero de threads a crear
    los otros parametros son con los cuales se construira la
    funcion target"""
    target = genera_target(iter_get_params, foo_do, foo_done)
    return [threading.Thread(target=target)
            for _ in range(n_threads)]


def descarga_varios(id_inicio, id_fin, n_threads=1):
    """Descargar un intervalo de candidatos utilizando varios
    threads"""
    if id_inicio > id_fin:
        iter_params = range(id_inicio, id_fin - 1, -1)
    else:
        iter_params = range(id_inicio, id_fin + 1)
    kargs = {"n_threads": n_threads,
             "iter_get_params": iter_params,
             "foo_do": descarga_candidato,
             "foo_done": db_inserta}
    lista_threads = genera_threads(**kargs)

    for hilo in lista_threads:
        hilo.setDaemon(True)
        hilo.start()

    while True:
        try:
            if not max([x.isAlive() for x in lista_threads]):
                break
            time.sleep(0.1)
        except KeyboardInterrupt:
            client.close()
            return


if __name__ == "__main__":
    # Descarga y filtra candidatos a la base de datos Mongodb
    descarga_varios(0, 116826, 2)
