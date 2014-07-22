#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
from maestro import Maestro
from recolecta import Recolector
from Queue import Queue


# Generador de parametros
gen_params = (tuple() for x in range(5))

# Generador de tareas
gen_tasks = ((x,) for x in range(104272, 104280))

# Crea objeto maestro
master = Maestro(5, Recolector, gen_params, gen_tasks)

# Inicia
print "Iniciando"
master.iniciar()

print "Finalizado"
