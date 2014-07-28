# -*- coding: iso-8859-15 -*-

_lista_keys = ["familia",
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

class Filtro (object):
    """Clase de funciones estaticas para filtrar la data

    Una funcion para cada api"""
    @staticmethod
    def f_principal(data):
        data = data["d"]
        if (not data["strDNI"]):
            return None
        proc_elect_BE = data["objProcesoElectoralBE"]
        ambo_BE = data["objAmbitoBE"]
        car_aut_BE = data["objCargoAutoridadBE"]
        ubi_pos_Be = data["objUbigeoPostulaBE"]
        ubi_nac_BE = data["objUbigeoNacimientoBE"]
        ubi_res_BE = data["objUbigeoResidenciaBE"]
        fec_nac = data["strFecha_Nac"][::-1]
        dic_postulacion = {
            "cargo": car_aut_BE["strCargoAutoridad"],
            "departamento": ubi_pos_Be["strDepartamento"],
            "provincia": ubi_pos_Be["strProvincia"],
            "distrito": ubi_pos_Be["strDistrito"],
            "designacion": data["strFormaDesignacion"],
        }
        dic_datos_personales = {
            "dni": int(data["strDNI"]),
            "apellidoPaterno": data["strAPaterno"].upper(),
            "apellidoMaterno": data["strAPaterno"].upper(),
            "nombres": data["strNombres"].upper(),
            "sexo": "M" if data["intId_Sexo"] == 1 else "F",
            "email": data["strCorreo"],
        }
        dic_nacimiento = {
            "fecha": "/".join([fec_nac[0:2], fec_nac[2:4], fec_nac[4:]]),
            "pais": data["strPais"],
            "departamento": ubi_nac_BE["strDepartamento"],
            "provincia": ubi_nac_BE["strProvincia"],
            "distrito": ubi_nac_BE["strDistrito"],
        }
        dic_residencia = {
            "lugar": data["strResidencia"],
            "departamento": ubi_res_BE["strDepartamento"],
            "provincia": ubi_res_BE["strProvincia"],
            "distrito": ubi_res_BE["strDistrito"],
            "tiempo": data["strTiempo_Residencia"],
        }
        return {
            "postulacion": dic_postulacion,
            "datosPersonales": dic_datos_personales,
            "nacimiento": dic_nacimiento,
            "residencia": dic_residencia,
        }

    @staticmethod
    def f_familia(data):
        data = data["d"]
        dic_keys = {
            1: "padre",
            2: "madre",
            3: "conyuge",
        }
        dic_familia = {}
        for item in data:
            tipo_key = item["objTipoBE"]["intTipo"]
            dic_familia[dic_keys[tipo_key]] = item["strNombres"].upper()

        return dic_familia if dic_familia else None

    @staticmethod
    def f_experiencia(data):
        data = data["d"]
        if (not data):
            return None
        lista_experiencia = []
        for item in data:
            ubi_exp = item["objUbigeoExperiencia"]
            dic_experiencia = {
                "empleador": item["strEmpleador"],
                "sector": item["objTipoSectorBE"]["strNombre_Sector"],
                "cargo": item["intInicioAnio"],
                "fin": item["intFinAnio"] if item["intFinAnio"] else "AHORA",
                "inicio": item["intInicioAnio"],
                "departamento": ubi_exp["strDepartamento"],
                "provincia": ubi_exp["strProvincia"],
                "distrito": ubi_exp["strDistrito"],
            }
            lista_experiencia.append(dic_experiencia)

        return lista_experiencia if lista_experiencia else None

    @staticmethod
    def f_educacionBasica(data):
        data = data["d"]
        if (not data):
            return None
        lista_primaria = []
        lista_secundaria = []
        for item in data:
            if (item["intTipoEducacion"] == 1):
                # Primaria
                ubi_pri = item["objUbigeoPrimaria"]
                dic_primaria = {
                    "instEducativa": item["strCentroPrimaria"],
                    "concluido": int(item["strPrimaria"]),
                    "inicio": item["intAnioInicioPrimaria"],
                    "fin": (item["intAnioFinPrimaria"]
                            if item["intAnioFinPrimaria"] else None),
                    "pais": item["strPais"]
                }
                if(item["strFgExtranjero"] == "1"):
                    dic_primaria["departamento"] = None
                    dic_primaria["provincia"] = None
                    dic_primaria["distrito"] = None
                else:
                    dic_primaria["departamento"] = ubi_pri["strDepartamento"]
                    dic_primaria["provincia"] = ubi_pri["strProvincia"]
                    dic_primaria["distrito"] = ubi_pri["strDistrito"]

                lista_primaria.append(dic_primaria)

            else:
                # Secundaria
                ubi_pri = item["objUbigeoSecundaria"]
                dic_secundaria = {
                    "instEducativa": item["strCentroSecundaria"],
                    "concluido": int(item["strSecundaria"]),
                    "inicio": item["intAnioInicioSecundaria"],
                    "fin": (item["intAnioFinSecundaria"]
                            if item["intAnioFinSecundaria"] else None),
                    "pais": item["strPais"]
                }

                if(item["strFgExtranjero"] == "1"):
                    dic_secundaria["departamento"] = None
                    dic_secundaria["provincia"] = None
                    dic_secundaria["distrito"] = None
                else:
                    dic_secundaria["departamento"] = ubi_pri["strDepartamento"]
                    dic_secundaria["provincia"] = ubi_pri["strProvincia"]
                    dic_secundaria["distrito"] = ubi_pri["strDistrito"]

                lista_secundaria.append(dic_secundaria)

        return {"primaria": (lista_primaria
                             if lista_primaria else None),
                "secundaria": (lista_secundaria
                               if lista_secundaria else None)}

    @staticmethod
    def f_educacionSuperior(data):
        data = data["d"]
        if (not data):
            return None
        lista_tecnico = []
        lista_universitario = []
        lista_postgrado = []
        for item in data:
            if (item["objTipoEstudioBE"]["intTipo"] == 1):
                # Tecnico
                ubi_BE = item["objUbigeoBE"]
                dic_tecnico = {
                    "instEducativa": item["strNombreCentro"],
                    "especialidad": item["strNombreEstudio"],
                    "curso": item["strNombreCarrera"],
                    "concluido": int(item["strFgConcluido"]),
                    "inicio": item["intAnioInicio"],
                    "fin": (item["intAnioFinal"]
                            if item["intAnioFinal"] else None),
                    "pais": item["strPais"],
                }

                if(item["strFgExtranjero"] == "1"):
                    dic_tecnico["departamento"] = None
                    dic_tecnico["provincia"] = None
                    dic_tecnico["distrito"] = None
                else:
                    dic_tecnico["departamento"] = ubi_BE["strDepartamento"]
                    dic_tecnico["provincia"] = ubi_BE["strProvincia"]
                    dic_tecnico["distrito"] = ubi_BE["strDistrito"]

                lista_tecnico.append(dic_tecnico)

            if (item["objTipoEstudioBE"]["intTipo"] == 3):
                # Universitario
                ubi_BE = item["objUbigeoBE"]
                if(item["strFgExtranjero"] == "1"):
                    depa = None
                    prov = None
                    dist = None
                else:
                    depa = ubi_BE["strDepartamento"]
                    prov = ubi_BE["strProvincia"]
                    dist = ubi_BE["strDistrito"]

                dic_universitario = {
                    "instEducativa": item["strNombreCentro"],
                    "facultad": item["strNombreEstudio"],
                    "carrera": item["strNombreCarrera"],
                    "concluido": int(item["strFgConcluido"]),
                    "inicio": item["intAnioInicio"],
                    "fin": (item["intAnioFinal"]
                            if item["intAnioFinal"] else None),
                    "pais": item["strPais"],
                    "gradoTitulo": item["strTipoGrado"],
                    "departamento": depa,
                    "provincia": prov,
                    "distrito": dist,
                }

                lista_universitario.append(dic_universitario)

            else:
                # Postgrado
                ubi_BE = item["objUbigeoBE"]
                if(item["strFgExtranjero"] == "1"):
                    depa = None
                    prov = None
                    dist = None
                else:
                    depa = ubi_BE["strDepartamento"]
                    prov = ubi_BE["strProvincia"]
                    dist = ubi_BE["strDistrito"]

                dic_tipo = {0: None,
                            1: "Maestria",
                            2: "Doctorado",
                            3: item["strOtroTipoDocumento"]}

                dic_postgrado = {
                    "instEducativa": item["strNombreCentro"],
                    "especialidad": item["strNombreEstudio"],
                    "concluido": int(item["strFgConcluido"]),
                    "inicio": item["intAnioInicio"],
                    "fin": (item["intAnioFinal"]
                            if item["intAnioFinal"] else None),
                    "pais": item["strPais"],
                    "gradoTitulo": item["strTipoGrado"],
                    "tipo": dic_tipo[item["intTipoPostgrado"]]
                }

                lista_postgrado.append(dic_postgrado)

        return {
            "tecnico": (lista_tecnico
                        if lista_tecnico else None),
            "universitario": (lista_universitario
                              if lista_universitario else None),
            "postgrado": (lista_postgrado
                          if lista_postgrado else None),
        }

    @staticmethod
    def f_partidario(data):
        data = data["d"]
        if (not data):
            return None
        lista_partidario = []
        for item in data:
            dic_partidario = {
                "orgPolitica": item["strOrganizacionPolitica"],
                "ambito": item["objAmbitoBE"]["strAmbito"],
                "cargo": item["strNombre_Cargo"],
                "inicio": item["intAnio_Inicio"],
                "fin": (item["intAnio_Final"]
                        if item["intAnio_Final"] else None),
            }
            lista_partidario.append(dic_partidario)

        return lista_partidario

    @staticmethod
    def f_eleccion(data):
        data = data["d"]
        if (not data):
            return None
        lista_eleccion = []
        for item in data:
            ubi_car_pop_BE = item["objUbigeoCargoPopularBE"]
            dic_eleccion = {
                "orgPolitica": item["strOrganizacionPolitica"],
                "ambito": item["objAmbitoBE"]["strAmbito"],
                "procesoElectoral": item["strProcesoElectoral"],
                "inicio": item["intAnioInicio"],
                "fin": (item["intAnioFinal"]
                        if item["intAnioFinal"] else None),
                "departamento": (ubi_car_pop_BE["strDepartamento"]
                                 if ubi_car_pop_BE
                                 ["strDepartamento"] else None),
                "provincia": (ubi_car_pop_BE["strProvincia"]
                              if ubi_car_pop_BE["strProvincia"] else None),
                "distrito": (ubi_car_pop_BE["strDistrito"]
                             if ubi_car_pop_BE["strDistrito"] else None),
                "cargo": (item["strOtroCargo"]
                          if item["objAmbitoBE"]["intIdAmbito"] == 6
                          else item["objCargoAutoridadBE"]
                          ["strCargoAutoridad"]),
            }
            lista_eleccion.append(dic_eleccion)

        return lista_eleccion

    @staticmethod
    def f_militancia(data):
        data = data["d"]
        if (not data):
            return None
        lista_renuncias = []
        for item in data:
            dic_renuncias = {
                "orgPolitica": item["strOrgPolitica"],
                "inicio": item["intAnioInicio"],
                "fin": item["intAnioFinal"] if item["intAnioFinal"] else None,
            }
            lista_renuncias.append(dic_renuncias)

        return lista_renuncias if lista_renuncias else None

    @staticmethod
    def f_penal(data):
        data = data["d"]
        if (not data):
            return None
        lista_penal = []
        for item in data:
            fecha = item["strFecha_Sentencia"]
            dic_penal = {
                "expediente": item["strExpediente"],
                "fechaSentencia": "/".join([fecha[0:2],
                                            fecha[2:4],
                                            fecha[4:]]),
                "juzgado": item["strJuzagado"],
                "delito": item["strAcusacion_Penal"],
                "fallo": item["strFallo"],
            }
            lista_penal.append(dic_penal)

        return lista_penal if lista_penal else None

    @staticmethod
    def f_civil(data):
        data = data["d"]
        if (not data):
            return None
        lista_civil = []
        for item in data:
            dic_civil = {
                "materia": item["objTipoMateriaBE"]["strMateria"],
                "expediente": item["strExpediente"],
                "juzgado": item["strJuzgado"],
                "materia": item["strMateria"],
                "fallo": item["strFallo"],
            }
            lista_civil.append(dic_civil)

        return lista_civil if lista_civil else None

    @staticmethod
    def f_otraExperiencia(data):
        data = data["d"]
        if (not data):
            return None
        lista_otra_exp = []
        for item in data:
            dic_otra_exp = {
                "cargo": item["strCargo"],
                "entidad": item["strInstitucion"],
                "inicio": item["intAnio_Inicio"],
                "fin": item["intAnio_Final"] if item["intAnio_Final"] else None
            }
            lista_otra_exp.append(dic_otra_exp)

        return lista_otra_exp if lista_otra_exp else None

    @staticmethod
    def f_ingresos(data):
        data = data["d"]
        total = (data["floRemuneracionTotal"] +
                 data["floRentaTotal"] + data["floOtrosTotal"])

        dic_ingresos = {
            "remuneracion": {
                "publico": data["floRemuneracionPublico"],
                "privado": data["floRemuneracionPrivado"],
                "total":   data["floRemuneracionTotal"],
            },
            "renta": {
                "publico": data["floRentaPublico"],
                "privado": data["floRentaPrivado"],
                "total":   data["floRentaTotal"],
            },
            "otros": {
                "publico": data["floOtrosPublico"],
                "privado": data["floOtrosPrivado"],
                "total":   data["floOtrosTotal"],
            },
            "total": total,
        }

        return dic_ingresos

    @staticmethod
    def f_bienes(data):
        data = data["d"]
        if (not data):
            return None
        lista_muebles = []
        lista_inmuebles = []
        for item in data:
            if item["intId_Bien"] == 1:
                # Inmuebles
                dic_inmuebles = {
                    "tipo": item["strNombre_Bien"],
                    "direccion": item["strDescripcion_Bien"],
                    "registro": item["strCaracteristicas_Bien"],
                    "valor": item["floValor_Bien"],
                }
                lista_inmuebles.append(dic_inmuebles)
            else:
                dic_muebles = {
                    # Bien
                    "bien": "Vehiculo" if item["intId_Bien"] == 2 else "Otro",
                    # Tipo de bien
                    "tipo": item["strNombre_Bien"],
                    # Descripción / Marca-Modelo-Año
                    "descripcion": item["strDescripcion_Bien"],
                    # Placa / Caracteristicas
                    "caracteristicas": item["strCaracteristicas_Bien"],
                    # Valor S/.
                    "valor": item["floValor_Bien"],
                }

        return {
            "muebles": lista_muebles if lista_muebles else None,
            "inmuebles": lista_inmuebles if lista_inmuebles else None
        }

    @staticmethod
    def f_acreencias(data):
        data = data["d"]
        if (not data):
            return None
        lista_acreencias = []
        for item in data:
            dic_acreencias = {
                "detalle": item["strDetalleAcreencia"],
                "monto": item["floTotalDeuda"],
            }
            lista_acreencias.append(dic_acreencias)
        return lista_acreencias if lista_acreencias else None

    @staticmethod
    def f_observaciones(data):
        data = data["d"]
        if (not data):
            return None
        lista_observaciones = []
        for item in data:
            dic_observaciones = {
                "referencia": item["strReferencia"],
                "anotacion": item["strObservacionCompleto"],
            }
            lista_observaciones.append(dic_observaciones)
        return lista_observaciones if lista_observaciones else None

    @staticmethod
    def f_data_sucia(data):
        """funcion para limpiar la data generada por el bug en
        descarga_candidato"""
        dic_limpio = {"_id": data["_id"], "ok": False}
        dic_principal = Filtro.f_principal(data)
        if not dic_principal:
            return dic_limpio
        else:
            dic_limpio["ok"] = True

        dic_limpio.update(dic_principal)
        
        for key in _lista_keys:
            dic_limpio[key] = data[key]

        return dic_limpio
