"""
Tarea 2 del Job: Silver_Transform
Lee Bronze, une vuelos con aerolíneas y aeropuertos, crea columnas derivadas,
imputa retrasos faltantes, calcula métricas por aerolínea y aeropuerto y escribe Silver.

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


def guardar_delta(spark, df, catalog, schema, tabla):
    nombre = f"{catalog}.{schema}.{tabla}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre))
    print(f"[Silver] {nombre}: {spark.table(nombre).count()} filas")


def agregar_columnas_derivadas(df):
    arr = F.col("arr_delay")
    return (df
        .withColumn("delay_total", F.col("dep_delay") + F.col("arr_delay"))
        .withColumn("on_time", F.when(arr.isNull(), F.lit(None)).otherwise(arr <= 15))
        .withColumn("distance_km", F.round(F.col("distance_miles") * F.lit(1.60934), 2))
        .withColumn("delay_category",
                    F.when(arr.isNull(), "Unknown")
                     .when(arr <= 0, "None")
                     .when(arr <= 15, "Short")
                     .when(arr <= 60, "Medium")
                     .otherwise("Long")))


def metricas_aerolinea(df):
    return (df.groupBy("airline_id", "airline_name")
              .agg(F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
                   F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
                   F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
                   F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
                   F.count("*").alias("num_flights"),
                   F.round(F.avg(F.col("on_time").cast("int")) * 100, 2).alias("pct_on_time")))


def puntualidad_aerolinea_por_origen(df):
    return (df.groupBy("airline_id", "airline_name", "origin_airport", "origin_city")
              .agg(F.count("*").alias("num_flights"),
                   F.round(F.avg(F.col("on_time").cast("int")) * 100, 2).alias("pct_on_time")))


def metricas_aeropuerto(df, aeropuertos):
    salidas = (df.groupBy(F.col("origin_airport").alias("airport_id"))
                 .agg(F.count("*").alias("num_departures"),
                      F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay_as_origin"),
                      F.round(F.avg(F.col("on_time").cast("int")) * 100, 2).alias("pct_on_time_as_origin")))
    llegadas = (df.groupBy(F.col("destination_airport").alias("airport_id"))
                  .agg(F.count("*").alias("num_arrivals"),
                       F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay_as_destination"),
                       F.round(F.avg(F.col("on_time").cast("int")) * 100, 2).alias("pct_on_time_as_destination")))
    return (aeropuertos.select("airport_id", "city", "country")
              .join(salidas, "airport_id", "left")
              .join(llegadas, "airport_id", "left")
              .withColumn("total_flights",
                          F.coalesce("num_departures", F.lit(0)) + F.coalesce("num_arrivals", F.lit(0))))


def main():
    args = get_args()
    catalog = args.catalog
    spark = SparkSession.builder.getOrCreate()

    # 1. Cargar Bronze
    flights_b = spark.table(f"{catalog}.bronze.flights")
    airlines_b = spark.table(f"{catalog}.bronze.airlines")
    airports_b = spark.table(f"{catalog}.bronze.airports")

    # 2. Joins
    origen = airports_b.select(F.col("airport_id").alias("origin_airport"),
                               F.col("city").alias("origin_city"),
                               F.col("country").alias("origin_country"))
    destino = airports_b.select(F.col("airport_id").alias("destination_airport"),
                                F.col("city").alias("destination_city"),
                                F.col("country").alias("destination_country"))
    flights_joined = (flights_b.drop("ingest_timestamp", "source_file")
        .join(F.broadcast(airlines_b.select("airline_id", "airline_name")), "airline_id", "left")
        .join(F.broadcast(origen), "origin_airport", "left")
        .join(F.broadcast(destino), "destination_airport", "left"))

    # 5. Validación y limpieza: imputar retrasos con la mediana de la aerolínea
    medianas = (flights_joined.groupBy("airline_id")
                .agg(F.percentile_approx("dep_delay", 0.5).alias("med_dep"),
                     F.percentile_approx("arr_delay", 0.5).alias("med_arr")))
    flights_validated = (flights_joined
        .join(F.broadcast(medianas), "airline_id", "left")
        .withColumn("delay_imputed", F.col("dep_delay").isNull() | F.col("arr_delay").isNull())
        .withColumn("dep_delay", F.coalesce("dep_delay", "med_dep"))
        .withColumn("arr_delay", F.coalesce("arr_delay", "med_arr"))
        .drop("med_dep", "med_arr")
        .na.fill("Unknown", subset=["airline_name", "origin_city", "origin_country",
                                    "destination_city", "destination_country"]))

    # 3. Columnas derivadas
    flights_silver = (agregar_columnas_derivadas(flights_validated)
        .withColumn("silver_timestamp", F.current_timestamp())
        .select("flight_id", "flight_date", "airline_id", "airline_name",
                "origin_airport", "origin_city", "origin_country",
                "destination_airport", "destination_city", "destination_country",
                "dep_delay", "arr_delay", "delay_total", "on_time", "delay_category",
                "distance_miles", "distance_km", "delay_imputed", "silver_timestamp"))

    nulos = flights_silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                                   for c in ["dep_delay", "arr_delay", "airline_name", "origin_city"]]).first()
    print(f"[Silver] nulos tras validación: {nulos.asDict()}")

    # 4 y 6. Agregaciones y guardado
    guardar_delta(spark, flights_silver, catalog, "silver", "flights_enriched")
    guardar_delta(spark, metricas_aerolinea(flights_silver), catalog, "silver", "airline_metrics")
    guardar_delta(spark, puntualidad_aerolinea_por_origen(flights_silver), catalog, "silver", "airline_origin_ontime")
    guardar_delta(spark, metricas_aeropuerto(flights_silver, airports_b), catalog, "silver", "airport_metrics")


if __name__ == "__main__":
    main()
