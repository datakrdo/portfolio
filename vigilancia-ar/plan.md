# Vigilancia epidemiológica AR — project invariants

Analytics engineering sobre Databricks (Unity Catalog, SQL warehouse serverless) con
dbt-databricks. Dominio: vigilancia de Dengue y Zika (SNVS 2.0), fuente abierta del
Ministerio de Salud. Ver `docs/data_profile.md` para el perfil completo de datos.

1. Grano de la fuente: evento × departamento de residencia × grupo etario × semana
   epidemiológica × año. `fct_casos_semanales` respeta ese grano exacto, no lo colapsa.
2. Dos eras de esquema (2018-2022 vs 2023+) conviven como dos `source` tables
   (`dengue_zika_legado`, `dengue_zika_actual`). La unificación de nombres de columna y
   codificación geográfica pasa una sola vez, en `int_dengue_zika_unificado`, nunca en los
   marts.
3. `id_depto_indec_residencia`/`id_prov_indec_residencia` (Georef API) son la clave
   geográfica canónica en `dim_geografia`; los nombres de texto libre de cada archivo
   fuente nunca se usan para joinear.
4. Filas con `sepi_min` nulo o `cantidad` nulo se descartan en staging con un test
   `not_null` explícito, no con un `WHERE` silencioso en un mart.
5. Semanas sin casos reportados se densifican (cross join evento × geo × semana) antes de
   calcular tasas o corredor endémico — un `NULL`/ausencia de fila nunca se confunde con
   cero casos real vs. dato no publicado todavía.
6. El corredor endémico (`mart_corredor_endemico`) usa percentiles de los 5 años
   epidemiológicos previos a la semana evaluada, nunca el año en curso.
7. Cada revisión sucesiva de un mismo período (2024 se publicó 3 veces) se preserva vía
   `snapshot`, nunca se sobrescribe — `mart_revisiones` depende de tener las versiones
   históricas.
8. Los marts (`fct_*`, `dim_*`) llevan `contract: enforced: true`; un cambio de tipo en
   staging que rompa el contrato falla el build, no llega silenciosamente al mart.
9. Todo modelo no trivial (staging con reglas de limpieza, cualquier intermediate o mart)
   tiene al menos un test genérico sobre su grano (`unique` compuesto) además de
   `not_null`/`relationships` en sus llaves.
10. `analyses/` responde preguntas de negocio concretas en SQL — nunca queries sueltas sin
    una pregunta que las motive documentada en el propio archivo.
