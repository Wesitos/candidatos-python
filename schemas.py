from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Date, create_engine
from pymongo import MongoClient, ASCENDING
from datetime import date

Base = declarative_base()
engine = create_engine("mysql+pymysql://candidatos:candidatos@localhost/candidatos?charset=utf8")
Session = sessionmaker(bind=engine)
session = Session()

class Candidato(Base):
    __tablename__ = "candidatos"

    id = Column(Integer, primary_key=True)

    # Datos Personales
    dni = Column(String(8))
    nombres = Column(String(300))
    apellidoMaterno = Column(String(300))
    apellidoPaterno = Column(String(300))
    sexo = Column(String(300))
    email = Column(String(300))

    # Familia
    madre = Column(String(300))
    conyuge = Column(String(300))
    padre = Column(String(300))

    #Postulacion
    postulacion_cargo = Column(String(300))
    postulacion_ubigeo = Column(String(300))
    postulacion_distrito = Column(String(300))
    postulacion_provincia = Column(String(300))
    postulacion_departamento = Column(String(300))
    postulacion_designacion = Column(String(300))

    # Nacimiento
    nacimiento_pais = Column(String(300))
    nacimiento_ubigeo = Column(String(300))
    nacimiento_fecha = Column(Date)
    nacimiento_distrito = Column(String(300))
    nacimiento_provincia = Column(String(300))
    nacimiento_departamento = Column(String(300))

    # Residencia
    residencia_lugar = Column(String(300))
    residencia_ubigeo = Column(String(300))
    residencia_distrito = Column(String(300))
    residencia_tiempo = Column(String(300))
    residencia_provincia = Column(String(300))
    residencia_departamento = Column(String(300))

    bienes_muebles = relationship("BienMueble", backref="candidato")
    bienes_inmuebles = relationship("BienInmueble", backref="candidato")
    otra_experiencia = relationship("OtraExperiencia", backref="candidato")
    militancia = relationship("Militancia", backref="candidato")
    civil = relationship("Civil", backref="candidato")
    educacion_basica_primaria = relationship("Primaria", backref="candidato")
    educacion_basica_secundaria = relationship("Secundaria", backref="candidato")
    educacion_superior_postgrado = relationship("Postgrado", backref="candidato")
    educacion_superior_universitario = relationship("Universitario", backref="candidato")
    educacion_superior_tecnico = relationship("Tecnico", backref="candidato")
    partidario = relationship("Partidario", backref="candidato")
    eleccion = relationship("Eleccion", backref="candidato")
    experiencia = relationship("Experiencia", backref="candidato")
    observaciones = relationship("Observacion", backref="candidato")

class BienMueble(Base):
    __tablename__ = 'bienes_muebles'
    id = Column(Integer, primary_key=True)
    bien = Column(String(300))
    tipo = Column(String(300))
    descripcion = Column(String(300))
    caracteristicas = Column(String(300))
    valor = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class BienInmueble(Base):
    __tablename__ = 'bienes_inmuebles'
    id = Column(Integer, primary_key=True)
    registro = Column(String(300))
    valor = Column(Integer)
    tipo = Column(String(300))
    direccion = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class OtraExperiencia(Base):
    __tablename__ = 'otra_experiencia'
    id = Column(Integer, primary_key=True)
    cargo = Column(String(300))
    entidad = Column(String(300))
    inicio = Column(Integer)
    fin = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Militancia(Base):
    __tablename__ = 'militancias'
    id = Column(Integer, primary_key=True)
    fin = Column(Integer)
    inicio = Column(Integer)
    orgPolitica = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Civil(Base):
    __tablename__ = 'civiles'
    id = Column(Integer, primary_key=True)
    materia = Column(String(300))
    expediente = Column(String(300))
    juzgado = Column(String(300))
    materia = Column(String(300))
    fallo = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Postgrado(Base):
    __tablename__ = 'educacion_superior_postgrado'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    gradoTitulo = Column(String(300))
    tipo = Column(String(300))
    inicio = Column(Integer)
    pais = Column(String(300))
    instEducativa = Column(String(300))
    fin = Column(Integer)
    especialidad = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Tecnico(Base):
    __tablename__ = 'educacion_superior_tecnico'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String(300))
    curso = Column(String(300))
    distrito = Column(String(300))
    especialidad = Column(String(300))
    departamento = Column(String(300))
    inicio = Column(Integer)
    pais = Column(String(300))
    instEducativa = Column(String(300))
    fin = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Universitario(Base):
    __tablename__ = 'educacion_superior_universitario'
    id = Column(Integer, primary_key=True)
    departamento = Column(String(300))
    pais = Column(String(300))
    concluido = Column(Boolean)
    gradoTitulo = Column(String(300))
    provincia = Column(String(300))
    fin = Column(Integer)
    facultad = Column(String(300))
    carrera = Column(String(300))
    inicio = Column(Integer)
    instEducativa = Column(String(300))
    distrito = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Partidario(Base):
    __tablename__ = 'partidario'
    id = Column(Integer, primary_key=True)
    cargo = Column(String(300))
    ambito = Column(String(300))
    fin = Column(Integer)
    orgPolitica = Column(String(300))
    inicio = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Eleccion(Base):
    __tablename__ = 'eleccion'
    id = Column(Integer, primary_key=True)
    procesoElectoral = Column(String(300))
    cargo = Column(String(300))
    provincia = Column(String(300))
    departamento = Column(String(300))
    inicio = Column(Integer)
    fin = Column(Integer)
    distrito = Column(String(300))
    ambito = Column(String(300))
    orgPolitica = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Experiencia(Base):
    __tablename__ = 'experiencia'
    id = Column(Integer, primary_key=True)
    sector = Column(String(300))
    cargo = Column(Integer)
    provincia = Column(String(300))
    empleador = Column(String(300))
    inicio = Column(Integer)
    distrito = Column(String(300))
    fin = Column(String(300))
    departamento = Column(String(300))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Secundaria(Base):
    __tablename__ ='educacion_basica_secundaria'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String(300))
    departamento = Column(String(300))
    distrito = Column(String(300))
    inicio = Column(Integer)
    pais = Column(String(300))
    instEducativa = Column(String(300))
    fin = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Primaria(Base):
    __tablename__ = 'educacion_basica_primaria'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String(300))
    departamento = Column(String(300))
    distrito = Column(String(300))
    inicio = Column(Integer)
    pais = Column(String(300))
    instEducativa = Column(String(300))
    fin = Column(Integer)

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

class Observacion(Base):
    __tablename__ = 'observaciones'
    id = Column(Integer, primary_key=True)
    referencia = Column(String(300))
    anotacion = Column(String(2000))

    id_candidato = Column(Integer, ForeignKey('candidatos.id'))

def crea_candidato_object(dict_candidato):
    # Arreglo de bug de estudios duplicados
    # Campos duplicados: especialidad, fin, inicio, instEducativa, pais
    # Campos en postgrado: gradoTitulo : ""; tipo: None
    if dict_candidato["educacionSuperior"]:
        postgrado_dirty = dict_candidato["educacionSuperior"]["postgrado"] if dict_candidato["educacionSuperior"]["postgrado"] else []
        tecnico_list = dict_candidato["educacionSuperior"]["tecnico"] if dict_candidato["educacionSuperior"]["tecnico"] else []
    else:
        postgrado_dirty = []
        tecnico_list = []

    postgrado_clean = []
    for item in postgrado_dirty:
        if item["tipo"] == None and item["gradoTitulo"]=='':
            dict_prueba_post = { key:item[key]
                                 for key in ["especialidad", "fin","inicio","instEducativa","pais"]}
            list_prueba_tec =  [{ key:d[key]
                                 for key in ["especialidad",
                                            "fin",
                                            "inicio",
                                            "instEducativa",
                                            "pais"]} for d in tecnico_list]
            if dict_prueba_post not in list_prueba_tec:
                postgrado_clean.append(item)
        else:
            postgrado_clean.append(item)

    dict_campos = {
        "id": dict_candidato["_id"],
        "bienes_muebles": [BienMueble(**x) for x in dict_candidato["bienes"]["muebles"]] if dict_candidato["bienes"] and dict_candidato["bienes"]["muebles"] else [],
        "bienes_inmuebles": [BienInmueble(**x) for x in dict_candidato ["bienes"] ["inmuebles"]] if dict_candidato["bienes"] and dict_candidato["bienes"]["inmuebles"] else [],
        "otra_experiencia": [OtraExperiencia(**x) for x in dict_candidato["otraExperiencia"]] if dict_candidato["otraExperiencia"] else [],
        "militancia": [Militancia(**x) for x in dict_candidato["militancia"]] if  dict_candidato["militancia"] else [],
        "civil": [Civil(**x) for x in dict_candidato["civil"]] if dict_candidato["civil"] else [],
        "educacion_basica_primaria": [Primaria(**x) for x in dict_candidato["educacionBasica"]["primaria"]] if dict_candidato["educacionBasica"] and dict_candidato["educacionBasica"]["primaria"] else [],
        "educacion_basica_secundaria": [Secundaria(**x) for x in dict_candidato["educacionBasica"]["secundaria"]] if dict_candidato["educacionBasica"] and dict_candidato["educacionBasica"]["secundaria"] else [],
        "educacion_superior_postgrado": [Postgrado(**x) for x in postgrado_clean],
        "educacion_superior_universitario": [Universitario(**x) for x in dict_candidato["educacionSuperior"]["universitario"]] if dict_candidato["educacionSuperior"] and dict_candidato["educacionSuperior"]["universitario"] else [],
        "educacion_superior_tecnico": [Tecnico(**x) for x in dict_candidato["educacionSuperior"]["tecnico"]] if dict_candidato["educacionSuperior"] and dict_candidato["educacionSuperior"]["tecnico"] else [],
        "partidario": [Partidario(**x) for x in dict_candidato["partidario"]] if dict_candidato["partidario"] else [],
        "eleccion": [Eleccion(**x) for x in dict_candidato["eleccion"]] if dict_candidato["eleccion"] else [],
        "experiencia": [Experiencia(**x) for x in dict_candidato["experiencia"]] if dict_candidato["experiencia"] else [],
        "observaciones": [Observacion(**x) for x in dict_candidato["observaciones"]] if dict_candidato["observaciones"] else [],
    }

    dict_campos.update(dict_candidato["datosPersonales"])
    dict_campos["dni"] = str(dict_campos["dni"]).zfill(8)

    if dict_candidato["familia"]:
        dict_campos.update(dict_candidato["familia"])

    for key,value in dict_candidato["postulacion"].iteritems():
        dict_campos["postulacion_" + key] = value

        dict_fecha_nac = dict_candidato["nacimiento"]["fecha"]

    for key,value in dict_candidato["nacimiento"].iteritems():
        if key=="fecha":
            dict_fecha = value
            dict_campos["nacimiento_fecha"] = date(value["y"], value["m"], value["d"])
        else:
            dict_campos["nacimiento_" + key] = value

    for key,value in dict_candidato["residencia"].iteritems():
        dict_campos["residencia_" + key] = value

    candidato_object = Candidato(**dict_campos)
    return candidato_object

def crea_esquemas():
    Base.metadata.create_all(engine)

def mongo_to_sqlalchemy(client=MongoClient(), db="candidatos", coleccion="candLimpio"):
    collect = client[db][coleccion]
    cursor = collect.find({"ok":True}).sort("_id", ASCENDING)
    for cand in cursor:
        cand_id = cand["_id"]
        session.add(crea_candidato_object(cand))
        if not cand_id % 1000:
            print "Procesado: %d"%cand_id
            session.commit()
    session.commit()
