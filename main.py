import os
import sqlalchemy
from sqlalchemy import text, update
from google.cloud.sql.connector import Connector, IPTypes
from flask import jsonify
from datetime import datetime, timedelta
import functions_framework

# Configuración del conector Cloud SQL para cada base de datos con instancias diferentes
def connect_with_connector(instance_connection_name: str, db_name: str) -> sqlalchemy.engine.base.Engine:
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    connector = Connector(ip_type)

    def getconn():
        try:
            return connector.connect(
                instance_connection_name,
                "pymysql",
                user=db_user,
                password=db_pass,
                db=db_name,
            )
        except Exception as e:
            print(f"Error al conectar a la base de datos {db_name}: {e}")
            raise e

    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
    )
    return pool

# Cloud Function principal para gestionar el escalamiento de incidentes
@functions_framework.http
def escalate_incidents(request):
    try:
        # Conectar a la base de datos de incidentes
        instance_connection_name_incidentes = os.environ["INSTANCE_CONNECTION_NAME_INCIDENTES"]
        engine_incidentes = connect_with_connector(instance_connection_name_incidentes, "incidentes")
        
        # Conectar a la base de datos de clientes
        instance_connection_name_clientes = os.environ["INSTANCE_CONNECTION_NAME_CLIENTES"]
        engine_clientes = connect_with_connector(instance_connection_name_clientes, "clientes")

        # Consultar incidentes abiertos
        with engine_incidentes.connect() as conn_incidentes:
            query_incidentes = text("SELECT id, cliente_id, fecha_creacion FROM incidente WHERE estado = 'abierto'")
            incidentes_abiertos = conn_incidentes.execute(query_incidentes).fetchall()
            print(f"Incidentes abiertos encontrados: {len(incidentes_abiertos)}")

            # Procesar cada incidente
            for incidente in incidentes_abiertos:
                incidente_id = incidente[0]  # Accede por índice
                cliente_id = incidente[1]    # Accede por índice
                fecha_creacion = incidente[2]  # Accede por índice y es de tipo date

                # Convertir fecha_creacion a datetime para calcular el tiempo transcurrido
                fecha_creacion_datetime = datetime.combine(fecha_creacion, datetime.min.time())

                print(f"\nProcesando incidente ID: {incidente_id}, Cliente ID: {cliente_id}")

                # Obtener escalation_time del cliente
                with engine_clientes.connect() as conn_clientes:
                    query_cliente = text("SELECT escalation_time FROM clientes WHERE id = :cliente_id")
                    escalation_time = conn_clientes.execute(query_cliente, {"cliente_id": cliente_id}).scalar()

                if escalation_time is not None:
                    # Calcular el tiempo transcurrido desde la creación del incidente
                    tiempo_transcurrido = datetime.utcnow() - fecha_creacion_datetime
                    horas_transcurridas = tiempo_transcurrido.total_seconds() / 3600
                    print(f"Horas transcurridas para el incidente ID {incidente_id}: {horas_transcurridas}")

                    # Verificar si el tiempo transcurrido supera el tiempo de escalación
                    if horas_transcurridas > escalation_time:
                        print(f"Escalando incidente ID: {incidente_id} a estado 'escalado'")
                        # Actualizar el estado del incidente a "escalado"
                        update_query = text("UPDATE incidente SET estado = 'escalado' WHERE id = :incidente_id")
                        conn_incidentes.execute(update_query, {"incidente_id": incidente_id})
                        conn_incidentes.commit()
                    else:
                        print(f"Incidente ID: {incidente_id} aún no ha superado el tiempo de escalación.")
                else:
                    print(f"Cliente ID {cliente_id} no tiene definido un tiempo de escalación.")

        print("Proceso de escalamiento completado.")
        return jsonify({"message": "Incidentes procesados correctamente"}), 200

    except Exception as e:
        print(f"Error durante el proceso de escalamiento: {e}")
        return jsonify({"error": f"Error procesando incidentes: {str(e)}"}), 500
