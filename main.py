from pymongo import MongoClient
from extrae import descarga_candidato, lock_print
import threading

client = MongoClient()
database = "local"
collection = "hojasVida"

def db_inserta(dato):
    db = client["default"]
    collect = db[collection]
    item_id = collect.insert(dato)
    with lock_print:
        print "Insertado id: %d"%item_id

class LockedIterator(object):
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

def genera_target(iter_get_params, foo_in, foo_out):
    """Decorador para generar el target"""
    safe_iterable = LockedIterator(iter_get_params)
    def target():
        for item in safe_iterable:
            res = foo_in(item)
            foo_out(res)

        print "Hilo terminado"

    return target

def genera_threads(n_threads, iter_get_params, foo_do, foo_done):
    """Construye y devuelve una lista de threads"""
    target = genera_target(iter_get_params, foo_do, foo_done)
    return [threading.Thread(target=target)
            for _ in range(n_threads)]


def descarga_varios(id_inicio, id_fin, n_threads=5):
    iter_params = range(id_inicio, id_fin)
    kargs = {"n_threads": n_threads,
             "iter_get_params": iter_params,
             "foo_do": descarga_candidato,
             "foo_done": db_inserta}
    lista_threads = genera_threads(**kargs)

    for hilo in lista_threads:
        hilo.setDaemon(True)
        hilo.start()
    for hilo in lista_threads:
        try:
            hilo.join()
        except KeyboardInterrupt:
            return

#-----
if __name__ == "__main__":
    descarga_varios(1, 5)
