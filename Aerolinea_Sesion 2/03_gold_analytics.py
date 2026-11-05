# 03_gold_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# Configuración
# =========================
CATALOG = "airline_mantenimiento"   # mismo catálogo de Bronze y Silver
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark = SparkSession.builder.getOrCreate()

# =========================
# 0. Asegurar schema Gold
# =========================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# =========================
# 1. Leer Silver
# =========================

vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
mantenimientos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

# =========================
# 2. Tabla de hechos: vuelos enriquecidos
# =========================
# Une cada vuelo con su aeronave y con los aeropuertos de origen y destino

origen = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("nombre").alias("origen_nombre"),
    F.col("ciudad").alias("origen_ciudad"),
    F.col("pais").alias("origen_pais"),
)

destino = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("destino_id"),
    F.col("nombre").alias("destino_nombre"),
    F.col("ciudad").alias("destino_ciudad"),
    F.col("pais").alias("destino_pais"),
)

fact_vuelos = (
    vuelos_slv
    .join(aeronaves_slv, on="aeronave_id", how="left")
    .join(origen, on="origen_id", how="left")
    .join(destino, on="destino_id", how="left")
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("ruta", F.concat_ws(" - ", F.col("origen_id"), F.col("destino_id")))
    .withColumn("es_internacional", F.col("origen_pais") != F.col("destino_pais"))
)

# =========================
# 3. KPI vuelos diarios
# =========================

kpi_vuelos_diarios = (
    fact_vuelos
    .groupBy("fecha")
    .agg(
        F.countDistinct("vuelo_id").alias("total_vuelos"),
        F.countDistinct("aeronave_id").alias("aeronaves_activas"),
        F.round(F.avg("duracion_min"), 2).alias("duracion_promedio_min"),
        F.sum("duracion_min").alias("minutos_volados"),
    )
    .orderBy("fecha")
)

# Distribución de vuelos por estado (porcentaje sobre el total del día)
w_dia = Window.partitionBy("fecha")

kpi_vuelos_estado = (
    fact_vuelos
    .groupBy("fecha", "estado")
    .agg(F.count("vuelo_id").alias("cantidad"))
    .withColumn("pct_dia", F.round(F.col("cantidad") / F.sum("cantidad").over(w_dia) * 100, 2))
    .orderBy("fecha", "estado")
)

# =========================
# 4. KPI rutas
# =========================

w_rutas = Window.orderBy(F.col("total_vuelos").desc())

kpi_rutas = (
    fact_vuelos
    .groupBy("ruta", "origen_id", "origen_ciudad", "destino_id", "destino_ciudad", "es_internacional")
    .agg(
        F.count("vuelo_id").alias("total_vuelos"),
        F.round(F.avg("duracion_min"), 2).alias("duracion_promedio_min"),
        F.countDistinct("aeronave_id").alias("aeronaves_distintas"),
    )
    .withColumn("ranking", F.dense_rank().over(w_rutas))
    .orderBy("ranking")
)

# =========================
# 5. KPI mantenimiento por aeronave
# =========================

uso_aeronave = (
    vuelos_slv
    .groupBy("aeronave_id")
    .agg(
        F.count("vuelo_id").alias("total_vuelos"),
        F.round(F.sum("duracion_min") / 60, 2).alias("horas_voladas"),
    )
)

mant_aeronave = (
    mantenimientos_slv
    .groupBy("aeronave_id")
    .agg(
        F.count("mantenimiento_id").alias("total_mantenimientos"),
        F.round(F.sum("costo_usd"), 2).alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.round(F.sum("duracion_hr"), 2).alias("horas_en_mantenimiento"),
        F.max("fecha").alias("ultimo_mantenimiento"),
    )
)

w_costo = Window.orderBy(F.col("costo_total_usd").desc_nulls_last())

kpi_mantenimiento_aeronave = (
    aeronaves_slv
    .join(uso_aeronave, on="aeronave_id", how="left")
    .join(mant_aeronave, on="aeronave_id", how="left")
    .fillna(0, subset=["total_vuelos", "horas_voladas", "total_mantenimientos",
                       "costo_total_usd", "horas_en_mantenimiento"])
    .withColumn("edad_anios", F.year(F.current_date()) - F.col("anio_fabricacion"))
    .withColumn("dias_desde_ultimo_mant", F.datediff(F.current_date(), F.col("ultimo_mantenimiento")))
    .withColumn(
        "costo_por_hora_vuelo_usd",
        F.when(F.col("horas_voladas") > 0,
               F.round(F.col("costo_total_usd") / F.col("horas_voladas"), 2))
    )
    .withColumn("ranking_costo", F.dense_rank().over(w_costo))
    .orderBy("ranking_costo")
)

# =========================
# 6. KPI mantenimiento mensual por tipo
# =========================

kpi_mantenimiento_mensual = (
    mantenimientos_slv
    .withColumn("mes", F.date_trunc("month", F.col("fecha")).cast("date"))
    .groupBy("mes", "tipo")
    .agg(
        F.count("mantenimiento_id").alias("total_mantenimientos"),
        F.round(F.sum("costo_usd"), 2).alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.round(F.sum("duracion_hr"), 2).alias("horas_totales"),
    )
    .orderBy("mes", "tipo")
)

# =========================
# 7. KPI por fabricante y modelo
# =========================

kpi_flota_modelo = (
    kpi_mantenimiento_aeronave
    .groupBy("fabricante", "modelo")
    .agg(
        F.count("aeronave_id").alias("aeronaves"),
        F.round(F.avg("edad_anios"), 1).alias("edad_promedio_anios"),
        F.sum("total_vuelos").alias("total_vuelos"),
        F.round(F.sum("horas_voladas"), 2).alias("horas_voladas"),
        F.round(F.sum("costo_total_usd"), 2).alias("costo_mant_total_usd"),
    )
    .withColumn(
        "costo_por_hora_vuelo_usd",
        F.when(F.col("horas_voladas") > 0,
               F.round(F.col("costo_mant_total_usd") / F.col("horas_voladas"), 2))
    )
    .orderBy(F.col("costo_mant_total_usd").desc())
)

# =========================
# 8. Validaciones de Calidad
# =========================

print("Vuelos sin aeronave en catálogo:", fact_vuelos.filter(F.col("modelo").isNull()).count())
print("Vuelos sin aeropuerto de origen:", fact_vuelos.filter(F.col("origen_nombre").isNull()).count())
print("Vuelos sin aeropuerto de destino:", fact_vuelos.filter(F.col("destino_nombre").isNull()).count())
print("Mantenimientos con costo negativo:", mantenimientos_slv.filter(F.col("costo_usd") < 0).count())

# =========================
# 9. Guardar en Gold
# =========================

tablas_gold = {
    "fact_vuelos": fact_vuelos,
    "kpi_vuelos_diarios": kpi_vuelos_diarios,
    "kpi_vuelos_estado": kpi_vuelos_estado,
    "kpi_rutas": kpi_rutas,
    "kpi_mantenimiento_aeronave": kpi_mantenimiento_aeronave,
    "kpi_mantenimiento_mensual": kpi_mantenimiento_mensual,
    "kpi_flota_modelo": kpi_flota_modelo,
}

for nombre, df in tablas_gold.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.{nombre}")

print("Gold Delta completada")
print(f"Tablas creadas/actualizadas en {CATALOG}.{GOLD_SCHEMA}: {', '.join(tablas_gold)}")
