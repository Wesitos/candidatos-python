import tornado.web
import tornado.ioloop
import tornado.escape
import tornado.gen
import motor

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

# Tornado Handler
class HojaVidaHandler(tornado.web.RequestHandler):
    """Maneja las peticiones de hojas de vida completas"""
    @tornado.gen.coroutine
    def get(self, id_cand):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        db = self.settings['db']
        document = yield db.candLimpio.find_one({'_id':int(id_cand)})
        self.write(document)


class DistritoHandler(tornado.web.RequestHandler):
    """Maneja las peticiones por lugar"""
    @tornado.gen.coroutine
    def get(self, str_dep, str_prov, str_dis):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        db = self.settings['db']
        cursor_alcalde = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": str_prov.upper(),
             "postulacion.distrito": str_dis.upper(),
             "postulacion.cargo": "ALCALDE DISTRITAL"},
            fields=["_id"])
        lista_alcalde = []
        while (yield cursor_alcalde.fetch_next):
            document = cursor_alcalde.next_object()
            lista_alcalde.append(document["_id"])

        cursor_regidor = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": str_prov.upper(),
             "postulacion.distrito": str_dis.upper(),
             "postulacion.cargo": "REGIDOR DISTRITAL"},
            fields=["_id"])
        lista_regidor = []
        while (yield cursor_regidor.fetch_next):
            document = cursor_regidor.next_object()
            lista_regidor.append(document["_id"])

        dic_ids = [{"cargo": "ALCALDE DISTRITAL",
                    "id": lista_alcalde},
                   {"cargo": "REGIDOR DISTRITAL",
                    "id": lista_regidor}]

        self.write(tornado.escape.json_encode(dic_ids))

class ProvHandler(tornado.web.RequestHandler):
    """Maneja las peticiones por lugar"""
    @tornado.gen.coroutine
    def get(self, str_dep, str_prov):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        db = self.settings['db']
        cursor_alcalde = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": str_prov.upper(),
             "postulacion.distrito": "",
             "postulacion.cargo": "ALCALDE PROVINCIAL"},
            fields=["_id"])
        lista_alcalde = []
        while (yield cursor_alcalde.fetch_next):
            document = cursor_alcalde.next_object()
            lista_alcalde.append(document["_id"])

        cursor_regidor = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": str_prov.upper(),
             "postulacion.distrito": "",
             "postulacion.cargo": "REGIDOR PROVINCIAL"},
            fields=["_id"])
        lista_regidor = []
        while (yield cursor_regidor.fetch_next):
            document = cursor_regidor.next_object()
            lista_regidor.append(document["_id"])

        dic_ids = [{"cargo": "ALCALDE PROVINCIAL",
                    "id": lista_alcalde},
                   {"cargo": "REGIDOR PROVINCIAL",
                    "id": lista_regidor}]

        self.write(tornado.escape.json_encode(dic_ids))

class RegionHandler(tornado.web.RequestHandler):
    """Maneja las peticiones por lugar"""
    @tornado.gen.coroutine
    def get(self, str_dep):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        db = self.settings['db']
        cursor_vice = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": "",
             "postulacion.distrito": "",
             "postulacion.cargo": "VICEPRESIDENTE REGIONAL"},
            fields=["_id"])
        lista_vice = []
        while (yield cursor_vice.fetch_next):
            document = cursor_vice.next_object()
            lista_vice.append(document["_id"])

        cursor_pres = db.candLimpio.find(
            {"postulacion.departamento": str_dep.upper(),
             "postulacion.provincia": "",
             "postulacion.distrito": "",
             "postulacion.cargo": "PRESIDENTE REGIONAL"},
            fields=["_id"])
        lista_pres = []
        while (yield cursor_pres.fetch_next):
            document = cursor_pres.next_object()
            lista_pres.append(document["_id"])

        dic_ids = [{"cargo": "PRESIDENTE REGIONAL",
                    "id": lista_pres},
                   {"cargo": "VICEPRESIDENTE REGIONAL",
                    "id": lista_vice}]
        self.write(tornado.escape.json_encode(dic_ids))

        ista_pres = []

_motor_db = motor.MotorClient().candidatos

application = tornado.web.Application(
    [(r"/all/([0-9]+)", HojaVidaHandler),
     (r"/region/([A-Za-z]+)", RegionHandler),
     (r"/provincia/([A-Za-z]+)/([A-Za-z]+)", ProvHandler),
     (r"/distrito/([A-Za-z]+)/([A-Za-z]+)/([A-Za-z]+)", DistritoHandler)], db = _motor_db)

def deploy_server():
    
    application.listen(8800, address="0.0.0.0")
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    deploy_server()
