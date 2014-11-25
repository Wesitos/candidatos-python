from pymongo import MongoClient, ASCENDING
import csv

def make_cand_resumen(cand_dict):
    datosPersonales = cand_dict["datosPersonales"]
    postulacion = cand_dict["postulacion"]
    nacimiento = cand_dict["nacimiento"]
    residencia = cand_dict["residencia"]
    experiencia = cand_dict["experiencia"] if cand_dict["experiencia"] else []
    civil = cand_dict["civil"] if cand_dict["civil"] else []
    penal = cand_dict["penal"] if cand_dict["penal"] else []
    ingresos = cand_dict["ingresos"]

    if cand_dict["bienes"]:
        muebles = cand_dict["bienes"]["muebles"]
        inmuebles = cand_dict["bienes"]["inmuebles"]
        if muebles == None:
            muebles = []
        if inmuebles == None:
            inmuebles = []
    else:
        muebles = []
        inmuebles = []

    if cand_dict["educacionBasica"]:
        primaria = cand_dict["educacionBasica"]["primaria"]
        secundaria = cand_dict["educacionBasica"]["secundaria"]
        if primaria == None:
            primaria = []
        if secundaria == None:
            secundaria = []
    else:
        primaria = []
        secundaria = []

    if cand_dict["educacionSuperior"]:
        tecnico = cand_dict["educacionSuperior"]["tecnico"]
        universitario = cand_dict["educacionSuperior"]["universitario"]
        postgrado = cand_dict["educacionSuperior"]["postgrado"]
        if tecnico == None:
            tecnico = []
        if universitario == None:
            universitario = []
        if postgrado == None:
            postgrado = []
    else:
        tecnico = []
        universitario = []
        postgrado = []

    ed_concluido = lambda ed_list: reduce(lambda x,y: x or y,
                                          map(lambda d: d["concluido"],ed_list), 0) if ed_list else ""

    educacion_basica_dict = {
        "primaria_concluida": ed_concluido(primaria),
        "secundaria_concluida": ed_concluido(secundaria),
    }

    educacion_superior_dict = {
        "tecnico_concluido": ed_concluido(tecnico),
        "universitario_concluido": ed_concluido(universitario),
        "postgrado_concluido": ed_concluido(postgrado),
    }

    suma_concluido = lambda ed_list: sum(map (lambda d: (d["fin"] if d["fin"] else 2014) - d["inicio"] + 1
                                              if d["concluido"] else 0,
                                              ed_list))

    n_empleos_sector = lambda sector: sum(map (lambda d: 1
                                               if d["sector"].lower() == sector.lower()
                                               else 0, experiencia))
    tiempo_sector = lambda sector: sum(map (lambda d: (d["fin"] if d["fin"] != "AHORA" else 2014) - d["inicio"]
                                            if d["sector"].lower() == sector
                                            else 0, experiencia))
    res_dict = {
        "id_candidato": cand_dict["_id"],
        "dni": str(datosPersonales ["dni"]).zfill(8),
        "postulacion_cargo": postulacion["cargo"],
        "nacimiento_anio": nacimiento["fecha"]["y"],
        "edad": 2014 - nacimiento["fecha"]["y"],
        "residencia_tiempo": residencia["tiempo"],
        "educacion_basica_suma": suma_concluido(primaria + secundaria),
        "educacion_superior_suma": suma_concluido(universitario + tecnico + postgrado),
        "empleos_sector_privado": n_empleos_sector("privado"),
        "empleos_sector_publico": n_empleos_sector(u"p\xfablico"), #  ._.
        "tiempo_sector_privado": tiempo_sector("privado"),
        "tiempo_sector_publico": tiempo_sector(u"p\xfablico"), #  ._.
        "penal_o_civil": 1 if len(penal) + len(civil) > 0 else 0,
        "ingreso_total": ingresos["total"],
        "total_bienes": sum(map (lambda d: d["valor"], muebles + inmuebles)),
    }

    res_dict.update(educacion_basica_dict)
    res_dict.update(educacion_superior_dict)
    return res_dict

def get_resumen_all(db, coleccion, client=MongoClient(), ):
    cursor = client[db][coleccion].find({"ok":True}).sort("_id", ASCENDING)
    return ( make_cand_resumen(cand_dict) for cand_dict in cursor)

def write_resumen(filename = "resumen.csv", **mongoOptions):
    with open(filename, 'w') as csvfile:
        fieldnames = [
            'id_candidato',
            'dni',
            'nacimiento_anio',
            'edad',
            'postulacion_cargo',
            'residencia_tiempo',
            'penal_o_civil',
            'primaria_concluida',
            'secundaria_concluida',
            'universitario_concluido',
            'postgrado_concluido',
            'tecnico_concluido',
            'educacion_superior_suma',
            'educacion_basica_suma',
            'empleos_sector_privado',
            'empleos_sector_publico',
            'tiempo_sector_publico',
            'tiempo_sector_privado',
            'total_bienes',
            'ingreso_total',
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for res in get_resumen_all(**mongoOptions):
            writer.writerow(res)
