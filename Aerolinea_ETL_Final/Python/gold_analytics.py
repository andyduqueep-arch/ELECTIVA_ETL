"""
Tarea 3 del Job: Gold_Analytics
Lee Silver, genera el análisis avanzado (esquema analytics), las tablas Gold de negocio
y la tabla de variables para ML, y aplica optimización Delta.

Parámetro: --catalog (por defecto "trabajo_final")
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="trabajo_final")
    args, _ = parser.parse_known_args()
    return args


def guardar_delta(spark, df, catalog, schema, tabla):
    nombre = f"{catalog}.{schema}.{tabla}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre))
    print(f"[Gold] {nombre}: {spark.table(nombre).count()} filas")


def pct_on_time():
    return F.round(F.avg(F.col("on_time").cast("int")) * 100, 2)


def promedio_retraso(df, *cols):
    return (df.groupBy(*cols)
              .agg(F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
                   F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
                   F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
                   F.count("*").alias("num_flights")))


def analisis_avanzado(spark, flights, catalog):
    """Apartado 3: resultados en el esquema analytics."""
    t = (flights
         .withColumn("month_name", F.date_format("flight_date", "MMMM"))
         .withColumn("day_name", F.date_format("flight_date", "EEEE")))

    w_airline = Window.partitionBy("airline_id").orderBy(F.col("delay_total").desc())
    ranked = (t.withColumn("row_number", F.row_number().over(w_airline))
               .withColumn("dense_rank", F.dense_rank().over(w_airline)))
    cols_top = ["airline_id", "airline_name", "flight_id", "flight_date", "origin_airport",
                "destination_airport", "dep_delay", "arr_delay", "delay_total", "row_number", "dense_rank"]

    airport_rank = (t.groupBy("origin_airport", "origin_city", "origin_country")
        .agg(F.round(F.avg("dep_delay"), 2).alias("avg_delay"),
             F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
             F.count("*").alias("num_departures"))
        .withColumn("airport_rank", F.dense_rank().over(Window.orderBy(F.col("avg_delay").desc()))))

    airline_month = (t.groupBy("airline_id", "airline_name", "month", "month_name")
        .agg(F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
             F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
             pct_on_time().alias("pct_on_time"),
             F.count("*").alias("total_flights")))

    resultados = {
        "delay_by_month": promedio_retraso(t, "month", "month_name"),
        "delay_by_weekday": promedio_retraso(t, "day_of_week", "day_name"),
        "delay_by_quarter": promedio_retraso(t, "quarter"),
        "top5_delays_row_number": ranked.filter("row_number <= 5").select(cols_top),
        "top5_delays_dense_rank": ranked.filter("dense_rank <= 5").select(cols_top),
        "airport_delay_ranking": airport_rank,
        "airline_month_metrics": airline_month,
    }
    for nombre, df in resultados.items():
        guardar_delta(spark, df, catalog, "analytics", nombre)


def capa_gold(spark, flights, catalog):
    """Apartado 4: tablas Gold de negocio y tabla de variables para ML."""
    delay_airline_airport_month = (flights
        .groupBy("year_month", "year", "month", "airline_id", "airline_name",
                 "origin_airport", "origin_city")
        .agg(F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
             F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
             F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
             pct_on_time().alias("pct_on_time"),
             F.count("*").alias("num_flights"))
        .withColumn("gold_timestamp", F.current_timestamp()))

    w_punct = Window.orderBy(F.col("pct_on_time").desc(), F.col("avg_arr_delay").asc())
    airport_punctuality = (flights
        .groupBy("origin_airport", "origin_city", "origin_country")
        .agg(F.count("*").alias("num_departures"),
             F.sum(F.col("on_time").cast("int")).alias("on_time_flights"),
             pct_on_time().alias("pct_on_time"),
             F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
             F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"))
        .withColumn("punctuality_rank", F.dense_rank().over(w_punct))
        .withColumn("gold_timestamp", F.current_timestamp()))

    pct_red = flights.agg(F.avg(F.col("on_time").cast("int")) * 100).first()[0]
    airline_kpi = (flights
        .groupBy("airline_id", "airline_name")
        .agg(F.count("*").alias("num_flights"),
             F.sum(F.col("on_time").cast("int")).alias("on_time_flights"),
             pct_on_time().alias("pct_on_time"),
             F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"))
        .withColumn("network_pct_on_time", F.round(F.lit(pct_red), 2))
        .withColumn("pp_vs_network", F.round(F.col("pct_on_time") - F.lit(pct_red), 2))
        .withColumn("kpi_rank", F.dense_rank().over(Window.orderBy(F.col("pct_on_time").desc())))
        .withColumn("gold_timestamp", F.current_timestamp()))

    ml_features = (flights
        .filter(~F.col("delay_imputed"))
        .select("flight_id", "flight_date", "year_month", "month", "day_of_week", "quarter",
                "airline_id", "origin_airport", "destination_airport",
                "dep_delay", "distance_km", "arr_delay")
        .withColumn("gold_timestamp", F.current_timestamp()))

    guardar_delta(spark, delay_airline_airport_month, catalog, "gold", "delay_by_airline_airport_month")
    guardar_delta(spark, airport_punctuality, catalog, "gold", "airport_punctuality_ranking")
    guardar_delta(spark, airline_kpi, catalog, "gold", "airline_ontime_kpi")
    guardar_delta(spark, ml_features, catalog, "gold", "flights_ml_features")


def optimizar(spark, catalog):
    """Apartado 4.2: optimización Delta."""
    zorder = {
        f"{catalog}.silver.flights_enriched": "flight_date, airline_id",
        f"{catalog}.gold.delay_by_airline_airport_month": "airline_id, origin_airport",
        f"{catalog}.gold.flights_ml_features": "airline_id, flight_date",
    }
    for tabla, columnas in zorder.items():
        spark.sql(f"OPTIMIZE {tabla} ZORDER BY ({columnas})")
        spark.sql(f"ANALYZE TABLE {tabla} COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"[Gold] optimizada {tabla}")
    for tabla in [f"{catalog}.gold.airport_punctuality_ranking", f"{catalog}.gold.airline_ontime_kpi"]:
        spark.sql(f"OPTIMIZE {tabla}")
        print(f"[Gold] optimizada {tabla}")


def main():
    args = get_args()
    catalog = args.catalog
    spark = SparkSession.builder.getOrCreate()

    flights = (spark.table(f"{catalog}.silver.flights_enriched")
        .withColumn("year", F.year("flight_date"))
        .withColumn("month", F.month("flight_date"))
        .withColumn("year_month", F.date_format("flight_date", "yyyy-MM"))
        .withColumn("day_of_week", F.dayofweek("flight_date"))
        .withColumn("quarter", F.quarter("flight_date")))

    analisis_avanzado(spark, flights, catalog)
    capa_gold(spark, flights, catalog)
    optimizar(spark, catalog)


if __name__ == "__main__":
    main()
