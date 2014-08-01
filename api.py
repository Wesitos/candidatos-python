import tornado.web
import tornado.ioloop
import tornado.escape
from pymongo import MongoClient

_client = None
_database = "candidatos"
_collection = "candLimpio"

def conectar_db(host="localhost", port=27017,
                db=None, collection=None):
    """Crea un cliente de la base de datos Mongodb"""
    global _client 
    _client = MongoClient(host, port)
    if db:
        global _database
        _database = db
    if collection:
        global _collection
        _collection = collection

_lista_keys = ["datosPersonales",
               "familia",
               "otraExperiencia",
               "observaciones",
               "ingresos",
               "experiencia",
               "educacionSuperior",
               "educacionBasica",
               "militancia",
               "eleccion",
               "partidario",
               "bienes",
               "penal",
               "civil",
               "acreencias"]


def get_id_cand(id_cand,fields=None):
    """Devuelve la hoja de vida de un candidato"""
    db = _client[_database]
    collect = db[_collection]
    kargs = {}
    if fields:
        kargs = {"fields": {_lista_keys[x] for x in fields}}
    dic_cand = collect.find_one({"_id": id_cand},**kargs)
    return dic_cand

# Tornado Handler
class HojaVidaHandler(tornado.web.RequestHandler):
    """Maneja las peticiones de hojas de vida completas"""
    def get(self, id_cand):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        response = get_id_cand(int(id_cand))
        self.write(response)

class ParcialHandler(tornado.web.RequestHandler):
    """Maneja las peticones de solo un campo de la hoja de vida"""
    def get(self, id_cand, id_key):
        id_cand = int(id_cand)
        id_key = int(id_key)
        self.set_header("Content-Type", "application/json; charset=utf-8")
        response = get_id_cand(id_cand, [id_key])
        self.write(response)

application = tornado.web.Application(
    [(r"/all/([0-9]+)", HojaVidaHandler),
     (r"/part/([0-9])+/([0-9]+)", ParcialHandler)])

def deploy_server():
    conectar_db()
    application.listen(8888, address="0.0.0.0")
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    deploy_server()
