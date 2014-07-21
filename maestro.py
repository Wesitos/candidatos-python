#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
from __future__ import print_function
import threading


class Maestro(object):
    """Genera varios hilos de ejecucion y los pone a trabajar

    Recibe el numero de hilos a generar, la clase de la cual se instanciaran
    estos (debe heredar de threading.Thread) y un generador de listas de 
    tareas o parametros para las tareas.
    
    Una tarea solo puede mandarse una vez"""
    lock_print = threading.Lock()
    lock_get_task = threading.Lock()
    lock_finalizado = threading.Lock()
    lock_put_task = threading.Lock()
    lista_threads = []
    activo= False

    def __init__(self, n_threads, ClaseThread, gen_tasks):
        self.gen_params = gen_params
        self.ClaseThread = ClaseThread
    
    def iniciar(self):
        """Crea e inicia los threads"""
        if(not self.activo):
            self.activo = True
            lista_threads = [ClaseThread() for x in range(self.n_threads) ]
            for hilo in lista_threads:
                hilo.setDaemon(True)
            for hilo in lista_threads:
                hilo.start()
        else:
            return
    
    def get_task(self):
        """Llamado por un thread para adquirir una nueva tarea"""
        with self.lock_get_task:
            try:
                return self.gen_params.next()
            except StopIteration:
                self.finalizado()
                return None

    def put_task(self, *args):
        """Llama a la funcion estatica put_task de la ClaseThread"""
        with lock_put_task:
            self.ClaseThread.put_task(*args);

    def finalizado(self):
        """Llamado por un thread o al mismo Maestro para terminar"""
        with self.lock_finalizado:
            for hilo in self.lista_threads:
                try:
                    thread.exit()
                except:
                    pass
    
    def imprime(self,*args, end_string='\n',sep_string =' '):
        with self.lock_print:
            print(*args,end=end_string,sep=sep_string)

 
