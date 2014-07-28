Candidatos-python
=================
Programa para descargar las hojas de vida de los candidatos a las elecciones municipales y regionales 2014

***

### Archivos

* extrae.py: Funciones para hacer peticiones a las apis del JNE.
* filtro.py: Funciones para limpiar la informacion devuelta por las apis del JNE.
* salida.py: Ejemplo de hoja de vida de un candidato despues de la limpieza.
* main.py: automatizacion del proceso de descarga, limpieza y volcado a una base de datos Mongodb.

### Descargar el programa

Usando `git` solo es necesario ejecutar este comando en terminal para clonar el repositorio.
```bash
git clone https://github.com/HackSpaceUni/candidatos-python.git
```
Este comando descargara todo el repositorio en la carpeta "candidatos-python".

### Dependencias

Es programa esta escrito para correr con python 2.7.x (aunque tambien deberia funcionar con la version 2.6.x). Para realizar las peticiones http, el programa utiliza el paquete `requests`. Para conectarse con la base de datos Mongodb utiliza el paquete `pymongo`.

Estos 2 paquetes se pueden instalar facilmente utilizando pip.
```bash
cd candidatos-python
sudo pip install -r requirements.txt
```

### Ejemplo de uso

Teniendo el id del candidato, se puede extraer toda su hoja de vida a un diccionario python utilizando la funcion `descarga_candidato`. Esta funcion descarga toda los campos de la hoja de vida a excepcion de la agrupacion politica y el numero de registro.
```python
from extrae import descarga_candidato
id_cand = 102472
data_cand = descarga_candidato(id_cand)
```

Si tienes Tor instalado y quieres hacer las descargas detrás de esta red
anonimizadora debes agregar un parámetro adicional:

```python
data_cand = descarga_candidato(id_cand, tor=True)
```

Si se desea descargar varios candidatos y ya se cuenta con una base de datos Mongodb en `localhost` escuchando en el puerto 27017 se pueden aprovechar las funciones definidas en `main.py`.
```python
import main
main.descarga_varios(1, 100, 2)
```
Esto descarga los candidatos con ids desde el 1 al 100 (incluyendo estos) utilizando 2 threads (descarga 2 candidatos en paralelo). Si se quiere utilizar otros parametros para conectarse a la base de datos, se puede llamar antes a la funcion `conectar_db`.
```python
main.conectar_db("wesitos.com", 6000)
```
