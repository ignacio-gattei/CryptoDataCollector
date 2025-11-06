from sqlalchemy import create_engine
import pandas as pd



# Crear la cadena de conexión (usando el dialecto PostgreSQL)
engine = create_engine(f"postgresql://2025_ignacio_gattei:1ERK9lnO17Xrh@redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com:5439/pda")

# --- OPCIÓN 1: Especificar el esquema en la consulta
query = "SELECT * FROM 2025_ignacio_gattei_schema.tabla_prueba LIMIT 10;"
df = pd.read_sql(query, engine)

print(df.head())

