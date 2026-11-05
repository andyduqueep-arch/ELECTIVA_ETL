"""
Tarea 1 del Job: Bronze_Ingestion
Lee los CSV del volumen de Unity Catalog, convierte tipos, elimina nulos en las claves,
agrega ingest_timestamp y escribe las tablas Delta de la capa Bronze.

Parámetro: --catalog (por defecto "trabajo_final")
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="trabajo_final")
    args, _ = parser.parse_known_args()
    return args


def leer_csv(spark, raw_path, nombre):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(f"{raw_path}/{nombre}")
            .withColumn("source_file", F.col("_metadata.file_name")))


def guardar_delta(spark, df, catalog, schema, tabla):
    nombre = f"{catalog}.{schema}.{tabla}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre))
    print(f"[Bronze] {nombre}: {spark.table(nombre).count()} filas")


def main():
    args = get_args()
    catalog = args.catalog
    raw_path = f"/Volumes/{catalog}/landing/raw_files"
    spark = SparkSession.builder.getOrCreate()

    # Estructura de Unity Catalog (idempotente)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in ["landing", "bronze", "silver", "analytics", "gold", "ml"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw_files")

    # 1. Cargar
    flights_raw = leer_csv(spark, raw_path, "flights.csv")
    airlines_raw = leer_csv(spark, raw_path, "airlines.csv")
    airports_raw = leer_csv(spark, raw_path, "airports.csv")

    # 2. Convertir tipos
    flights = (flights_raw
        .withColumn("flight_date", F.to_date("flight_date", "yyyy-MM-dd"))
        .withColumn("dep_delay", F.col("departure_delay").cast("double").cast("int"))
        .withColumn("arr_delay", F.col("arrival_delay").cast("double").cast("int"))
        .withColumn("distance_miles", F.col("distance_miles").cast("int"))
        .withColumn("airline_id", F.trim(F.upper("airline_id")))
        .withColumn("origin_airport", F.trim(F.upper("origin_airport")))
        .withColumn("destination_airport", F.trim(F.upper("destination_airport")))
        .select("flight_id", "flight_date", "airline_id", "origin_airport",
                "destination_airport", "dep_delay", "arr_delay", "distance_miles", "source_file"))

    airlines = airlines_raw.select(F.trim(F.upper("airline_id")).alias("airline_id"),
                                   F.trim("airline_name").alias("airline_name"), "source_file")
    airports = airports_raw.select(F.trim(F.upper("airport_id")).alias("airport_id"),
                                   F.trim("city").alias("city"),
                                   F.trim("country").alias("country"), "source_file")

    # 3. Eliminar nulos en claves (y duplicados)
    antes = flights.count()
    flights = (flights.na.drop(subset=["flight_id", "airline_id", "origin_airport"])
                      .dropDuplicates(["flight_id"]))
    airlines = airlines.na.drop(subset=["airline_id"]).dropDuplicates(["airline_id"])
    airports = airports.na.drop(subset=["airport_id"]).dropDuplicates(["airport_id"])
    print(f"[Bronze] vuelos eliminados por nulos/duplicados: {antes - flights.count()}")

    # 4. ingest_timestamp
    ts = F.current_timestamp()
    flights = flights.withColumn("ingest_timestamp", ts)
    airlines = airlines.withColumn("ingest_timestamp", ts)
    airports = airports.withColumn("ingest_timestamp", ts)

    # 5. Guardar en Delta
    guardar_delta(spark, flights, catalog, "bronze", "flights")
    guardar_delta(spark, airlines, catalog, "bronze", "airlines")
    guardar_delta(spark, airports, catalog, "bronze", "airports")


if __name__ == "__main__":
    main()
