# Capturas de pantalla del Job y del flujo completo

Pantallazos del trabajo final. El documento completo está en `Trabajo_Final_ETL_Electiva.docx`.

## 1. Carpeta del trabajo final en el Workspace

Carpeta ARQUITECTURA_AEROLINEA_TRABAJO FINAL con las subcarpetas Python y Notebook.

![Carpeta del trabajo final en el Workspace](01_Carpeta_Workspace_Trabajo_Final.png)

## 2. Ejecución del Job con los archivos .py

Las tres tareas (bronze, silver y gold) en cadena, con estado Succeeded.

![Ejecución del Job con los archivos .py](02_Ejecucion_Job_Python_Timeline.png)

## 3. Notebook de los puntos 1, 2, 3 y 4

Notebooks de preparación, Bronze, Silver, Gold y Machine Learning.

![Notebook de los puntos 1, 2, 3 y 4](03_Notebook_puntos_1_2_3_4.png)

## 4. Jobs & Pipelines del notebook con los puntos 1, 2, 3 y 4

Job con las tareas 01_Preparacion a 04_Capa_Gold y sus ejecuciones exitosas.

![Jobs & Pipelines del notebook con los puntos 1, 2, 3 y 4](04_Jobs_Pipelines_Notebook_puntos_1_2_3_4.png)

## 5. Ficheros .py de bronce, plata y oro

bronze_ingestion.py, silver_transform.py y gold_analytics.py en el Workspace.

![Ficheros .py de bronce, plata y oro](05_Ficheros_py_bronce_plata_oro.png)

## 6. Jobs & Pipelines de los archivos .py

Se realizan 2 Run para comprobar su funcionamiento.

![Jobs & Pipelines de los archivos .py](06_Jobs_Pipelines_archivos_py_2_runs.png)

## 7. Archivos .PY y YAML

Los tres scripts y el YAML exportado del Job.

![Archivos .PY y YAML](07_Archivos_PY_y_YAML.png)

## 8. Orden en Unity Catalog: esquemas

Catálogo aerolinea_trabajo_final con sus esquemas.

![Orden en Unity Catalog: esquemas](08_Orden_Unity_Catalog_esquemas.png)

## 9. Orden en Unity Catalog: volumen de carga

Volumen landing.raw_files con los tres CSV.

![Orden en Unity Catalog: volumen de carga](09_Orden_Unity_Catalog_volumen_raw_files.png)

## 10. Orden en Unity Catalog: analytics y bronze

Tablas de los esquemas analytics y bronze.

![Orden en Unity Catalog: analytics y bronze](10_Orden_Unity_Catalog_analytics_bronze.png)

## 11. Orden en Unity Catalog: gold y landing

Tablas de gold y archivos del volumen en landing.

![Orden en Unity Catalog: gold y landing](11_Orden_Unity_Catalog_gold_landing.png)

## 12. Notebook y resultados del punto 6 Machine Learning

Notebook 05_Machine_Learning y esquema ml con tablas y modelo registrado.

![Notebook y resultados del punto 6 Machine Learning](12_ML_Notebook_Apartado_6.png)

## 13. Notebook y resultados del punto 6 Machine Learning

Métricas de los modelos y predicciones del modelo registrado.

![Notebook y resultados del punto 6 Machine Learning](13_ML_Resultados_predicciones.png)

## 14. Notebook y resultados del punto 6 Machine Learning

Esquema ml (tablas, función y modelo) y tablas de silver.

![Notebook y resultados del punto 6 Machine Learning](14_ML_Unity_Catalog_ml_silver.png)
