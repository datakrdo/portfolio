---
title: Metodología
---

# Metodología, fuentes y límites

## De dónde salen los números

- **Casos:** [Vigilancia de Dengue y Zika](https://datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika), Ministerio de Salud de la Nación (SNVS 2.0), licencia CC-BY 4.0. Se publica con un esquema distinto antes y después de 2023, y algunos años se republicaron varias veces con cifras distintas.
- **Población:** [Estructura de población identificada](https://datos.gob.ar/dataset/renaper-estructura-poblacion-argentina), RENAPER, CC-BY 4.0. Es población con DNI, no una proyección del INDEC.
- **Geografía:** códigos de departamento y provincia de [Georef](https://datosgobar.github.io/georef-ar-api/). Contornos provinciales de [Natural Earth](https://www.naturalearthdata.com/) (dominio público).
- **Actualización:** una vez por semana se busca si hay archivos nuevos o modificados; si no los hay, el sitio no se reconstruye.

## Cómo se calcula

- **Tasa:** casos del período cada 100.000 habitantes, usando el corte de población más cercano al año.
- **Corredor endémico:** percentiles 25, 50 y 75 de la misma semana en los 5 años previos, por provincia y evento. No incluye el año evaluado. Los años con menos de 5 años de historia usan los que haya.
- **Brote:** racha de semanas consecutivas por encima del percentil 75. Una semana sin base histórica no cuenta como brote.
- **Código y pruebas:** todo el procesamiento es SQL versionado con dbt. La [documentación del modelo de datos](https://datakrdo.github.io/portfolio/dbt/) incluye el linaje completo.

## Qué no dice este sitio

- **Residencia, no contagio.** Los casos se asignan al departamento de residencia, que puede no ser donde la persona se infectó.
- **Las últimas semanas están incompletas.** Las notificaciones llegan con demora; ver el hallazgo sobre [retraso de notificación](./hallazgos#retraso-de-notificacion).
- **CABA se publica por ciudad.** RENAPER informa la población de CABA completa, no por comuna: las comunas aparecen sin tasa.
- **Sin apertura por edad.** Los conteos pequeños por departamento, edad y semana podrían identificar personas, por eso este sitio publica solo agregados por departamento o provincia.
- **Notificado no es confirmado.** Las cifras son casos notificados al sistema, con criterios que pueden variar entre jurisdicciones y años.
