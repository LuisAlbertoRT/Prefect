# Pipeline ETL de Control de Riesgo Financiero con Prefect y Pandas

Este repositorio contiene la implementación de un pipeline de datos local estructurado bajo la arquitectura clásica **ETL (Extract, Transform, Load)**. El objetivo principal es simular la ingesta de transacciones financieras crudas desde una base de datos de producción (legada/operativa), aplicar reglas rigurosas de limpieza de calidad y auditoría de riesgo mediante **Pandas**, y consolidar la información en un **Data Warehouse analítico** indexado en tablas relacionales utilizando **SQLite3**.

El flujo y control de las tareas está modularizado utilizando el orquestador **Prefect**, asegurando la resiliencia del pipeline ante fallas externas mediante políticas automatizadas de reintentos.

---

## 🏗️ Arquitectura y Flujo de Datos (Data Lineage)

El pipeline opera de manera agnóstica dividiendo las responsabilidades en componentes altamente especializados (`@task`), garantizando que los datos crudos muten controladamente hacia estructuras analíticas óptimas:



1. **Extracción (`extract_from_source`)**: Consume transacciones en crudo provenientes de `produccion_banco.db`. Maneja una política de tolerancia a fallas de 3 reintentos analizando la conexión con el almacenamiento.
2. **Transformación (`transform_and_evaluate_risk`)**: Ejecuta un motor de reglas de negocio para mitigar riesgos e imperfecciones del sistema origen:
   * **Calidad de Datos**: Remoción de duplicaciones exactas de registros y normalización de formatos cronológicos (`YYYY-MM-DD`).
   * **Limpieza Monetaria**: Casteo de strings financieros (ej. `$15,200.00`) a tipos flotantes numéricos puros. Tratamiento de valores nulos (`NaN`/`None`) mediante imputación neutra (`0.0`).
   * **Regla de Riesgo**: Clasificación algorítmica de registros sospechosos. Si una transacción supera el umbral parametrizado (ej. `$40,000.00`) o carece de monto original, activa la bandera `alto_riesgo = 1`.
3. **Carga (`load_to_warehouse`)**: Realiza una carga persistente e idempotente en `data_warehouse.db`,
