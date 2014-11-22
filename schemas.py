from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Date, create_engine
from pymongo import MongoClient()


Base = declarative_base()

class Candidato(Base):
    __tablename__ = "candidatos"

    id = Column(Integer)

    # Datos Personales
    dni = Column(Integer, primary_key=True)
    nombres = Column(String)
    apellidoMaterno = Column(String)
    apellidoPaterno = Column(String)
    sexo = Column(String)
    email = Column(String)

    # Familia
    madre = Column(String)
    conyuge = Column(String)
    padre = Column(String)

    #Postulacion
    postulacion_cargo = Column(String)
    postulacion_ubigeo = Column(String)
    postulacion_distrito = Column(String)
    postulacion_provincia = Column(String)
    postulacion_departamento = Column(String)
    postulacion_designacion = Column(String)

    # Nacimiento
    nacimiento_pais = Column(String)
    nacimiento_ubigeo = Column(String)
    nacimiento_fecha = Column(Date)
    nacimiento_distrito = Column(String)
    nacimiento_provincia = Column(String)
    nacimiento_departamento = Column(String)

    # Residencia
    residencia_lugar = Column(String)
    residencia_ubigeo = Column(String)
    residencia_distrito = Column(String)
    residencia_tiempo = Column(String)
    residencia_provincia = Column(String)
    residencia_departamento = Column(String)

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

class BienMueble(Base):
    __tablename__ = 'bienes_muebles'
    id = Column(Integer, primary_key=True)
    bien = Column(String)
    tipo = Column(String)
    descripcion = Column(String)
    caracteristicas = Column(String)
    valor = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class BienInmueble(Base):
    __tablename__ = 'bienes_inmuebles'
    id = Column(Integer, primary_key=True)
    registro = Column(String)
    valor = Column(Integer)
    tipo = Column(String)
    direccion = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class OtraExperiencia(Base):
    __tablename__ = 'otra_experiencia'
    id = Column(Integer, primary_key=True)
    cargo = Column(String)
    entidad = Column(String)
    inicio = Column(Integer)
    fin = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Militancia(Base):
    __tablename__ = 'militancias'
    id = Column(Integer, primary_key=True)
    fin = Column(Integer)
    inicio = Column(Integer)
    orgPolitica = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Civil(Base):
    __tablename__ = 'civiles'
    id = Column(Integer, primary_key=True)
    materia = Column(String)
    expediente = Column(String)
    juzgado = Column(String)
    materia = Column(String)
    fallo = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Postgrado(Base):
    __tablename__ = 'educacion_superior_postgrado'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    gradoTitulo = Column(String)
    tipo = Column(String)
    inicio = Column(Integer)
    pais = Column(String)
    instEducativa = Column(String)
    fin = Column(Integer)
    especialidad = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Tecnico(Base):
    __tablename__ = 'educacion_superior_tecnico'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String)
    curso = Column(String)
    distrito = Column(String)
    especialidad = Column(String)
    departamento = Column(String)
    inicio = Column(Integer)
    pais = Column(Integer)
    instEducativa = Column(String)
    fin = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Universitario(Base):
    __tablename__ = 'educacion_superior_universitario'
    id = Column(Integer, primary_key=True)
    departamento = Column(String)
    pais = Column(String)
    concluido = Column(Boolean)
    gradoTitulo = Column(String)
    provincia = Column(String)
    fin = Column(Integer)
    facultad = Column(String)
    carrera = Column(String)
    inicio = Column(Integer)
    instEducativa = Column(String)
    distrito = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Partidario(Base):
    __tablename__ = 'partidario'
    id = Column(Integer, primary_key=True)
    cargo = Column(String)
    ambito = Column(String)
    fin = Column(Integer)
    orgPolitica = Column(String)
    inicio = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Eleccion(Base):
    __tablename__ = 'eleccion'
    id = Column(Integer, primary_key=True)
    procesoElectoral = Column(String)
    cargo = Column(String)
    provincia = Column(String)
    departamento = Column(String)
    inicio = Column(Integer)
    fin = Column(Integer)
    distrito = Column(String)
    ambito = Column(String)
    orgPolitica = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Experiencia(Base):
    __tablename__ = 'experiencia'
    id = Column(Integer, primary_key=True)
    sector = Column(String)
    cargo = Column(Integer)
    provincia = Column(String)
    empleador = Column(String)
    inicio = Column(Integer)
    distrito = Column(String)
    fin = Column(String)
    departamento = Column(String)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Secundaria(Base):
    __tablename__ ='educacion_basica_secundaria'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String)
    departamento = Column(String)
    distrito = Column(String)
    inicio = Column(Integer)
    pais = Column(String)
    instEducativa = Column(String)
    fin = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

class Primaria(Base):
    __tablename__ = 'educacion_basica_primaria'
    id = Column(Integer, primary_key=True)
    concluido = Column(Boolean)
    provincia = Column(String)
    departamento = Column(String)
    distrito = Column(String)
    inicio = Column(Integer)
    pais = Column(String)
    instEducativa = Column(String)
    fin = Column(Integer)

    dni_candidato = Column(Integer, ForeignKey('candidatos.dni'))

def crea_candidato_object(dict_candidato):
    dict_campos = {
        "id": dict_candidato["_id"],
        "bienes_muebles": [BienMueble(**x) for x in dict_candidato["bienes"]["muebles"]] if dict_candidato["bienes"] and dict_candidato["bienes"]["muebles"] else [],
        "bienes_inmuebles": [BienInmueble(**x) for x in dict_candidato ["bienes"] ["inmuebles"]] if dict_candidato["bienes"] and dict_candidato["bienes"]["inmuebles"] else [],
        "otra_experiencia": [OtraExperiencia(**x) for x in dict_candidato["otraExperiencia"]] if dict_candidato["otraExperiencia"] else [],
        "militancia": [Militancia(x) for x in dict_candidato["militancia"]] if  dict_candidato["militancia"] else [],
        "civil": [Civil(**x) for x in dict_candidato["civil"]] if dict_candidato["civil"] else [],
        "educacion_basica_primaria": [Primaria(**x) for x in dict_candidato["educacionBasica"]["primaria"]] if dict_candidato["educacionBasica"] and dict_candidato["educacionBasica"]["primaria"] else [],
        "educacion_basica_secundaria": [Secundaria(**x) for x in dict_candidato["educacionBasica"]["secundaria"]] if dict_candidato["educacionBasica"] and dict_candidato["educacionBasica"]["secundaria"] else [],
        "educacion_superior_postgrado": [Postgrado(**x) for x in dict_candidato["educacionSuperior"]["postgrado"]] if dict_candidato["educacionSuperior"] and dict_candidato["educacionSuperior"]["postgrado"] else [],
        "educacion_superior_universitario": [Universitario(**x) for x in dict_candidato["educacionSuperior"]["universitario"]] if dict_candidato["educacionSuperior"] and dict_candidato["educacionSuperior"]["universitario"] else [],
        "educacion_superior_tecnico": [Tecnico(**x) for x in dict_candidato["educacionSuperior"]["tecnico"]] if dict_candidato["educacionSuperior"] and dict_candidato["educacionSuperior"]["tecnico"] else [],
        "partidario": [Partidario(**x) for x in dict_candidato["partidario"]] if dict_candidato["partidario"] else [],
        "eleccion": [Eleccion(**x) for x in dict_candidato["eleccion"]] if dict_candidato["eleccion"] else [],
        "experiencia": [Experiencia(**x) for x in dict_candidato["experiencia"]] if dict_candidato["experiencia"] else [],
    }
    dict_campos.update(dict_candidato["datosPersonales"])
    dict_campos.update(dict_candidato["familia"])
    for key,value in dict_candidato["postulacion"].iteritems():
        dict_campos["postulacion_" + key] = value

    for key,value in dict_candidato["nacimiento"].iteritems():
        dict_campos["nacimiento_" + key] = value

    for key,value in dict_candidato["residencia"].iteritems():
        dict_campos["residencia_" + key] = value

    candidato_object = Candidato(**dict_campos)
    return candidato_object

def mongo_to_sqlalchemy(client=MongoClient(), db="candidatos", colleccion="candLimpio"):
    collect = client.db.collect
    cursor = collect.find({"ok":True})
    for cand in cursor:
        Base.add(crear_candidato_object(cand))
