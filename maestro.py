#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
from __future__ import print_function
import threading
import time


class Maestro(object):
    """Genera varios hilos de ejecucion y los pone a trabajar

    Recibe el numero de hilos a generar, la clase de la cual se instanciaran
    estos (debe heredar de threading.Thread), un iterador para los parametros
    con que se contruiran los threads y un generador de listas de tareas
    (parametros que necesitan los threads para trabajar y que van pidiendo
    constantemente). Una tarea solo puede mandarse una vez"""

    def __init__(self, n_threads, ClaseThread, done, thread_params, gen_tasks):
        self.lock_print = threading.Lock()
        self.lock_get_task = threading.Lock()
        self.lock_finalizado = threading.Lock()
        self.n_threads = n_threads
        self.thread_params = thread_params
        self.gen_tasks = gen_tasks
        self.ClaseThread = ClaseThread
        self.activo = False
        if !ClaseThread.block_put_task:
            self.lock_put_task = None

    def iniciar(self, block=True):
        """Crea e inicia los threads"""
        if(not self.activo):
            self.activo = True
            self.lista_threads = [
                self.ClaseThread(maestro=self, *y) for x, y
                in zip(range(self.n_threads), self.thread_params)]
            for hilo in self.lista_threads:
                hilo.setDaemon(True)
                hilo.start()
            # Bloquea la ejecucion
            if(block):
                try:
                    self.imprime("Maestro: Bloqueando")
                    for hilo in self.lista_threads:
                        hilo.join()
                except KeyboardInterrupt:
                    for hilo in self.lista_threads:
                            hilo.exit()
                    return

        else:
            return

    def get_task(self):
        """Llamado por un thread para adquirir una nueva tarea"""        
        with self.lock_get_task:
            return self.gen_tasks.next()

    def finalizado(self):
        """Llamado por un thread o el mismo maestro para
        indicar que ha finalizado"""
        with self.lock_finalizado:
            pass

    def imprime(self, *args):
        with self.lock_print:
            print(*args)
