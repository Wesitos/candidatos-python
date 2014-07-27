Candidatos-python
=================
Programa para descargar las hojas de vida de los candidatos a las elecciones municipales y regionales 2014

***

### Archivos

* extrae.py: Funciones para hacer peticiones a las apis dej JNE
* filtro.py: Funciones para limpiar la informacion devuelta por las apis del JNE
* salida.py: Ejemplo de hoja de vida de un candidato despues de la limpieza
* main.py: automatizacion del proceso de descarga, limpieza y volcado a una base de datos mongodb

### Ejemplo de uso

Teniendo el id del candidato, se puede extraer toda su hoja de vida a un diccionario python utilizando la funcion `descarga_candidato`

```python
from extrae import descarga_candidato
id_cand = 102472
data_cand = descarga_candidato(id_cand)
```
