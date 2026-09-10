# Proyecto: Análisis de Sentimiento en Letras de Canciones (Portfolio Data Scientist)

## 1. Contexto y objetivos

Este documento define, de forma precisa y no ambigua, cómo implementar un proyecto de análisis de sentimientos sobre letras de canciones, segmentado por álbum y periodo temporal, con posibilidad de comparar bandas (ej. The Cure vs The Smiths). El stack debe ser 100% gratuito y open source, y el sistema de análisis de sentimientos debe estar actualizado al estado del arte en 2026.

El documento está pensado para que cualquier LLM pueda continuar el desarrollo simplemente siguiendo estas instrucciones, sin tomar decisiones implícitas ni ambiguas.

## 2. Preguntas de investigación

El sistema debe permitir responder, como mínimo, las siguientes preguntas:

1. ¿Qué tan “tristes” o “alegres” son las letras de una banda en un periodo de tiempo dado?
2. ¿Cómo varía el sentimiento a lo largo del tiempo? ¿La banda se vuelve más deprimente o más alegre en su trayectoria?
3. ¿Qué diferencias de sentimiento existen entre álbumes de una misma banda?
4. ¿Cómo se compara el sentimiento de las letras de dos bandas distintas (ej. The Cure vs The Smiths) en el mismo periodo de tiempo?

## 3. Principios de diseño obligatorios

1. **Stack 100% gratuito y open source**: no usar servicios de pago ni librerías propietarias.
2. **Reproducibilidad total**: todas las decisiones deben estar codificadas en código fuente versionado con Git, archivos de configuración (`config.yaml`) y documentación.
3. **Separación de capas**: adquisición de datos, limpieza y preprocesado, análisis NLP, agregación y visualización deben ser módulos independientes.
4. **Respeto por copyright**: no publicar letras completas en el repositorio público; conservarlas localmente y publicar únicamente resultados derivados, identificadores y agregados.
5. **Reutilización responsable**: reutilizar código externo solo tras revisar licencia, atribución requerida y compatibilidad con el proyecto.

## 4. Repositorios externos a reutilizar

### 4.1. LyricLens

- **Repositorio:** `sgroenjes/LyricLens` en GitHub.
- Propósito: herramienta de análisis de sentimiento musical que integra NLP y modelos de machine learning para evaluar letras.
- Uso recomendado:
  - Revisar y adaptar patrones de arquitectura para separar adquisición, procesamiento, inferencia y presentación.
  - Evaluar su integración con Genius y Spotify para inspirar los clientes de fuentes de datos.
  - Revisar sus rutinas de limpieza basadas en NLTK y spaCy.
- No reutilizar su modelo de sentimiento como dependencia principal sin evaluarlo frente al protocolo de validación descrito en este documento.

### 4.2. NLP Song Lyrics

- **Proyecto:** “NLP Song Lyrics”, publicado como tutorial/proyecto que utiliza `lyricsgenius` y modelos de Hugging Face.
- Uso recomendado:
  - Adaptar la lógica de obtención de letras con `lyricsgenius`.
  - Adaptar la estructura de funciones como `get_song_lyrics`, `clean_song_lyrics` y el patrón de inferencia multi-modelo.
  - Revisar su tratamiento de etiquetas estructurales de letras, como `[Verse]`, `[Chorus]` y `[Bridge]`.
- No copiar resultados, datasets ni claves; implementar un pipeline propio, reproducible y con pruebas.

### 4.3. Lyric-Analysis-Project

- **Repositorio:** `mcat18/Lyric-Analysis-Project`.
- Propósito: análisis de texto, sentimiento y topic modeling de letras, con agrupación por álbum.
- Uso recomendado:
  - Extraer ideas para modelos de datos a nivel canción y álbum.
  - Dejar topic modeling como mejora posterior, no como requisito del MVP.

### 4.4. Criterio de adopción de código externo

Antes de incorporar código de un repositorio externo:

1. Confirmar que su licencia permite reutilización.
2. Añadir un archivo `THIRD_PARTY_NOTICES.md` con nombre, URL, commit o release revisado, licencia y partes adaptadas.
3. Copiar solo módulos o patrones necesarios; no incorporar dependencias innecesarias ni credenciales.
4. Escribir tests para todo código adaptado.
5. Documentar cualquier cambio respecto al original.

## 5. Stack tecnológico y versiones (agosto 2026)

### 5.1. Python

- Usar Python `>=3.11,<3.13`.
- Preferir Python 3.12 si está disponible y todas las dependencias resuelven correctamente.
- Usar un entorno virtual por proyecto (`venv`, `uv`, Poetry o Conda). Para simplicidad y velocidad, se recomienda `uv`.

### 5.2. Librerías principales

- **PyTorch:** `torch>=2.11,<2.15`.
- **Transformers:** `transformers>=5.16.1,<6.0.0`.
- **Cliente Genius:** `lyricsgenius>=3.6.4`.
- **Manipulación de datos:** `pandas>=2.0.0`, `numpy>=1.24.0`.
- **Visualización:** `matplotlib>=3.8.0`, `seaborn>=0.13.0`, `plotly>=5.18.0`.
- **Utilidades:** `tqdm>=4.66.0`, `python-dotenv>=1.0.0`.

### 5.3. Dependencias adicionales recomendadas

- `pyyaml>=6.0.0` para configuración.
- `pyarrow>=15.0.0` para leer y escribir Parquet.
- `scipy>=1.11.0` para regresiones, pruebas estadísticas e intervalos.
- `scikit-learn>=1.4.0` para métricas, validación y baselines.
- `statsmodels>=0.14.0` para modelos estadísticos e intervalos robustos.
- `pydantic>=2.0.0` para validación de configuraciones y contratos de datos.
- `tenacity>=8.0.0` para reintentos controlados de APIs.
- `rapidfuzz>=3.0.0` para matching aproximado de títulos, álbumes y artistas.
- `pytest>=8.0.0`, `ruff>=0.5.0`, `mypy>=1.0.0` y `pre-commit>=3.0.0` para calidad de código.
- `streamlit>=1.35.0` para dashboard opcional.

### 5.4. Archivo `requirements.txt`

```text
python>=3.11,<3.13

torch>=2.11,<2.15
transformers>=5.16.1,<6.0.0
lyricsgenius>=3.6.4

pandas>=2.0.0
numpy>=1.24.0
pyarrow>=15.0.0
scipy>=1.11.0
scikit-learn>=1.4.0
statsmodels>=0.14.0

matplotlib>=3.8.0
seaborn>=0.13.0
plotly>=5.18.0
streamlit>=1.35.0

tqdm>=4.66.0
python-dotenv>=1.0.0
pyyaml>=6.0.0
pydantic>=2.0.0
tenacity>=8.0.0
rapidfuzz>=3.0.0

pytest>=8.0.0
ruff>=0.5.0
mypy>=1.0.0
pre-commit>=3.0.0
```

### 5.5. Regla de actualización de dependencias

1. Antes de comenzar una nueva fase, ejecutar `pip list --outdated` o el equivalente de `uv`.
2. No actualizar paquetes a ciegas en medio de un análisis reproducible.
3. Probar el pipeline y los tests después de cada actualización.
4. Congelar versiones exactas para una entrega reproducible con un archivo lock (`uv.lock`, `poetry.lock` o `requirements.lock`).
5. Documentar en `CHANGELOG.md` la fecha, versión anterior, versión nueva y resultado de pruebas.

## 6. Modelo de datos y fuentes

### 6.1. Fuente de letras

La fuente principal será Genius mediante la API y el cliente `lyricsgenius`.

Reglas:

1. La API requiere token, almacenado exclusivamente en `.env`.
2. Nunca subir `.env` al repositorio.
3. Configurar pausas entre peticiones, cache local, reintentos con backoff y logging de errores.
4. Guardar URL fuente y timestamp de extracción para trazabilidad.
5. Tratar la ausencia de letra, álbum o fecha como datos faltantes explícitos, nunca como valores inventados.

### 6.2. Fuente de metadata

Usar Genius como fuente inicial de título, artista, álbum y fecha. Para enriquecer o validar metadata, evaluar MusicBrainz y/o Spotify solo si su uso no rompe el requisito de gratuidad y no depende de scraping no permitido.

La fecha canónica para análisis temporal será, en orden de preferencia:

1. Fecha de lanzamiento del álbum oficial.
2. Fecha de lanzamiento del single oficial.
3. Año de publicación proporcionado por la fuente.
4. Si no existe fecha verificable, la canción se excluye del análisis temporal y se conserva para análisis no temporal.

## 7. Arquitectura del repositorio

```text
lyrics-sentiment-analysis/
├── .env.example
├── .gitignore
├── README.md
├── LICENSE
├── THIRD_PARTY_NOTICES.md
├── CHANGELOG.md
├── pyproject.toml
├── requirements.txt
├── config/
│   ├── config.yaml
│   └── artists.yaml
├── data/
│   ├── raw/                 # Ignorado por Git: respuestas y letras originales
│   ├── interim/             # Ignorado por Git: datos intermedios
│   ├── processed/           # Resultados procesados; publicar solo derivados permitidos
│   └── cache/               # Ignorado por Git: cache de APIs
├── notebooks/
│   ├── 01_fetch_lyrics.ipynb
│   ├── 02_quality_checks.ipynb
│   ├── 03_analyze_sentiment.ipynb
│   └── 04_visualize_patterns.ipynb
├── src/
│   └── lyrics_sentiment/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── schemas.py
│       ├── lyrics_fetcher.py
│       ├── metadata_resolver.py
│       ├── data_processor.py
│       ├── sentiment_analyzer.py
│       ├── aggregations.py
│       ├── statistics.py
│       ├── visualizer.py
│       └── io.py
├── tests/
│   ├── test_fetcher.py
│   ├── test_processor.py
│   ├── test_chunking.py
│   ├── test_sentiment.py
│   ├── test_aggregations.py
│   └── test_statistics.py
└── output/
    ├── figures/
    ├── tables/
    └── dashboard_exports/
```

## 8. Configuración reproducible

### 8.1. Archivo `config/config.yaml`

```yaml
project:
  name: "lyrics-sentiment-analysis"
  random_seed: 42
  language: "en"

paths:
  raw_dir: "data/raw"
  interim_dir: "data/interim"
  processed_dir: "data/processed"
  cache_dir: "data/cache"
  output_dir: "output"

acquisition:
  source: "genius"
  request_sleep_seconds: 1.0
  max_retries: 3
  retry_backoff_seconds: 2.0
  timeout_seconds: 30
  save_raw_responses: true

analysis:
  sentiment_model: "cardiffnlp/twitter-roberta-base-sentiment-latest"
  emotion_model: null
  batch_size: 16
  max_tokens: 512
  min_chars_for_analysis: 30
  use_gpu_if_available: true
  score_formula: "positive_minus_negative"

aggregation:
  min_songs_per_album: 3
  min_songs_per_year: 3
  chunk_aggregation: "token_weighted_mean"
  temporal_aggregation: "song_mean"

statistics:
  confidence_level: 0.95
  bootstrap_iterations: 5000
  alpha: 0.05
  multiple_testing_correction: "fdr_bh"

visualization:
  dpi: 300
  style: "whitegrid"
  compare_periods:
    - [1980, 1990]
    - [1990, 2000]
```

### 8.2. Archivo `config/artists.yaml`

```yaml
artists:
  - name: "The Cure"
    max_songs: 150
  - name: "The Smiths"
    max_songs: 100
  - name: "Radiohead"
    max_songs: 150
```

Regla: ningún parámetro analítico, umbral o rango temporal debe estar hardcodeado en scripts de producción. Debe leerse desde archivos YAML.

## 9. Esquema de datos

### 9.1. Tabla `songs_raw`

Guardar localmente en `data/raw/songs_raw.parquet` o JSONL. No publicar si incluye letras completas.

Campos mínimos:

| Campo | Tipo | Descripción |
|---|---|---|
| `song_id` | string | UUID interno estable |
| `artist` | string | Nombre canónico de artista |
| `song_title` | string | Título reportado por fuente |
| `album` | string/null | Álbum reportado por fuente |
| `release_date` | date/null | Fecha canónica si se conoce |
| `year` | integer/null | Año derivado de `release_date` |
| `lyrics` | string/null | Letra original; dato sensible por copyright |
| `source_name` | string | Ej.: `genius` |
| `source_url` | string | URL de procedencia |
| `retrieved_at` | datetime | Timestamp UTC de extracción |
| `fetch_status` | string | `success`, `not_found`, `error`, `partial` |
| `error_message` | string/null | Diagnóstico si falla |

### 9.2. Tabla `songs_processed`

Guardar en `data/processed/songs_processed.parquet`.

| Campo | Tipo | Descripción |
|---|---|---|
| `song_id` | string | Referencia a canción original |
| `artist` | string | Artista |
| `song_title` | string | Título |
| `album` | string/null | Álbum |
| `release_date` | date/null | Fecha canónica |
| `year` | integer/null | Año para análisis temporal |
| `lyrics_char_count` | integer | Longitud antes de limpieza |
| `cleaned_char_count` | integer | Longitud tras limpieza |
| `token_count` | integer | Tokens del tokenizer del modelo |
| `chunk_count` | integer | Número de segmentos analizados |
| `is_eligible_for_sentiment` | boolean | Cumple condiciones mínimas |
| `exclusion_reason` | string/null | Motivo de exclusión |

### 9.3. Tabla `song_sentiment`

Guardar en `data/processed/song_sentiment.parquet`.

| Campo | Tipo | Descripción |
|---|---|---|
| `song_id` | string | Identificador de canción |
| `model_name` | string | Checkpoint exacto usado |
| `model_revision` | string/null | Commit/revisión si se registra |
| `negative` | float | Probabilidad agregada negativa |
| `neutral` | float | Probabilidad agregada neutral |
| `positive` | float | Probabilidad agregada positiva |
| `dominant_sentiment` | string | `negative`, `neutral` o `positive` |
| `sentiment_score` | float | `positive - negative`, rango teórico [-1, 1] |
| `analyzed_at` | datetime | Timestamp UTC |

### 9.4. Tabla `album_summary`

Campos mínimos: `artist`, `album`, `album_year`, `song_count`, `mean_sentiment_score`, `std_sentiment_score`, `median_sentiment_score`, `ci_low`, `ci_high`.

### 9.5. Tabla `year_summary`

Campos mínimos: `artist`, `year`, `song_count`, `mean_sentiment_score`, `std_sentiment_score`, `median_sentiment_score`, `ci_low`, `ci_high`.

## 10. Obtención de letras

### 10.1. Variables de entorno

Crear `.env.example`:

```env
GENIUS_ACCESS_TOKEN="REEMPLAZAR_CON_TU_TOKEN"
```

Crear `.env` localmente:

```env
GENIUS_ACCESS_TOKEN="token_real_no_versionado"
```

### 10.2. Responsabilidades de `lyrics_fetcher.py`

Implementar una clase `LyricsFetcher` que:

1. Cargue `GENIUS_ACCESS_TOKEN` desde variables de entorno.
2. Detenga la ejecución con un error claro si el token no está disponible.
3. Inicialice `lyricsgenius.Genius` con espera entre peticiones y logging reducido.
4. Recupere canciones por artista.
5. Almacene metadata, letra, URL, estado y fecha de extracción.
6. Use cache para no repetir peticiones exitosas.
7. Reintente únicamente errores transitorios de red.
8. No silencie errores: registrar diagnóstico en `error_message`.

Firma sugerida:

```python
class LyricsFetcher:
    def get_artist_songs(self, artist_name: str, max_songs: int) -> list[dict]:
        ...

    def save_raw_records(self, records: list[dict], output_path: str) -> None:
        ...
```

### 10.3. Controles de calidad de adquisición

Para cada artista, generar una tabla con:

- Número de canciones solicitadas.
- Número de canciones obtenidas.
- Número con letra no vacía.
- Número con álbum.
- Número con año.
- Número excluido y motivos.
- Número de duplicados detectados.

No iniciar el análisis de sentimiento hasta revisar estas métricas.

## 11. Limpieza y preprocesado

### 11.1. Principio fundamental

No eliminar palabras emocionalmente importantes. No eliminar negaciones (`not`, `never`, `no`), intensificadores (`very`, `so`, `too`) ni pronombres si pueden aportar contexto.

### 11.2. Función `clean_lyrics`

Implementar:

```python
def clean_lyrics(text: str) -> str:
    ...
```

Orden obligatorio de operaciones:

1. Validar que `text` sea string no vacío.
2. Normalizar Unicode.
3. Eliminar encabezados estructurales entre corchetes, como `[Verse 1]`, `[Chorus]`, `[Bridge]`, `[Outro]`, `[Instrumental]`.
4. Eliminar anotaciones técnicas y créditos que no sean contenido lírico.
5. Reemplazar múltiples espacios, tabulaciones y saltos de línea por un único espacio.
6. Conservar contracciones y apóstrofes dentro de palabras.
7. No eliminar puntuación antes de probar el modelo base; muchos tokenizers modernos manejan puntuación y contexto.
8. No aplicar stopword removal para el modelo transformer principal.
9. Guardar tanto el texto original como el limpio solamente en archivos locales no publicados si contienen letras completas.

### 11.3. Duplicados y versiones

Implementar detección de duplicados por:

1. Normalización de artista y título.
2. Similaridad aproximada del título con `rapidfuzz`.
3. Similaridad de letra limpia si está disponible.

Marcar explícitamente:

- `is_live_version`
- `is_remix`
- `is_demo`
- `is_reissue`
- `is_duplicate_candidate`

Para el análisis principal, incluir únicamente versiones de estudio cuando sea posible. Las versiones live/remix/demo deben ser excluidas o analizadas en una cohorte separada.

## 12. Segmentación de letras largas

### 12.1. Problema

Los clasificadores transformer tienen un límite máximo de tokens, típicamente 512. Las letras largas no deben truncarse sin registrar pérdida de información.

### 12.2. Función requerida

```python
def chunk_lyrics(text: str, tokenizer, max_tokens: int) -> list[dict]:
    ...
```

Cada elemento debe incluir:

- `chunk_index`
- `text`
- `token_count`

### 12.3. Algoritmo obligatorio

1. Separar primero por líneas o bloques semánticos.
2. Acumular bloques hasta acercarse a `max_tokens`.
3. Si un bloque individual supera el límite, dividir por oraciones; si no hay oraciones, dividir por tokens.
4. Nunca exceder `max_tokens` después de tokenizar.
5. Registrar número de tokens de cada chunk.

### 12.4. Agregación de chunks

Para una canción con chunks `i = 1, ..., n`, calcular cada probabilidad agregada mediante promedio ponderado por tokens:

\[
p_{cancion} = \frac{\sum_{i=1}^{n} token_count_i \cdot p_i}{\sum_{i=1}^{n} token_count_i}
\]

Después calcular:

\[
sentiment\_score = positive - negative
\]

No promediar simplemente los chunks sin ponderación, salvo que se justifique y compare en una prueba de sensibilidad.

## 13. Análisis de sentimiento

### 13.1. Modelo principal

Usar como baseline principal:

```text
cardiffnlp/twitter-roberta-base-sentiment-latest
```

El modelo devuelve probabilidades para tres clases:

- `negative`
- `neutral`
- `positive`

El score continuo obligatorio será:

```text
sentiment_score = positive - negative
```

Rango teórico: [-1, 1].

Interpretación:

- Cerca de -1: señal negativa alta.
- Cerca de 0: neutralidad o incertidumbre relativa.
- Cerca de +1: señal positiva alta.

No interpretar automáticamente `negative` como “deprimente” sin validación manual específica para letras musicales.

### 13.2. Clase `LyricsSentimentAnalyzer`

Implementar:

```python
class LyricsSentimentAnalyzer:
    def analyze_sentiment(self, text: str) -> dict:
        ...

    def analyze_batch(self, texts: list[str], batch_size: int) -> list[dict]:
        ...
```

Salida exacta por canción:

```python
{
    "negative": float,
    "neutral": float,
    "positive": float,
    "dominant_sentiment": "negative" | "neutral" | "positive",
    "sentiment_score": float,
}
```

### 13.3. Reglas de inferencia

1. Usar `AutoTokenizer` y `AutoModelForSequenceClassification`.
2. Colocar el modelo en `cuda` solo si está disponible y está habilitado en configuración; si no, usar CPU.
3. Ejecutar inferencia con `torch.no_grad()` o `torch.inference_mode()`.
4. Aplicar `softmax` a los logits.
5. Validar el orden de etiquetas leyendo `model.config.id2label`; no asumir el orden sin comprobarlo.
6. Guardar nombre del modelo, versión de librería, device y fecha de inferencia.

### 13.4. Modelo de emociones opcional

En una fase posterior, incorporar un modelo de emociones para diferenciar, por ejemplo, tristeza, ira, miedo, alegría y amor.

Reglas:

1. No reemplazar el modelo principal sin una evaluación comparativa.
2. Guardar los scores por emoción en una tabla separada.
3. No mezclar escalas de distintos modelos sin estandarización y documentación.
4. Tratar emociones como dimensiones complementarias, no como sustituto directo de sentimiento valencia positivo/negativo.

## 14. Validación manual y calidad del modelo

### 14.1. Necesidad de validación

Un modelo entrenado en tweets puede no generalizar perfectamente a letras musicales. Metáforas, narradores ficticios, ironía, repetición de estribillos y temas oscuros con tono musical alegre generan riesgo de error.

### 14.2. Muestra de evaluación

1. Seleccionar al menos 100 canciones de distintos artistas, décadas y niveles de score.
2. Muestrear estratificadamente entre predicción negativa, neutral y positiva.
3. Excluir canciones con letra incompleta.
4. Anotar manualmente por dos personas independientes si es posible.

### 14.3. Guía de anotación

Clasificar cada letra según la valencia expresada por el texto completo:

- `negative`: predominan tristeza, desesperanza, dolor, rabia, amenaza o desesperación.
- `neutral`: no predomina una valencia positiva o negativa, o hay mezcla equilibrada.
- `positive`: predominan alegría, esperanza, afecto, celebración, alivio o gratitud.
- `uncertain`: contexto insuficiente, letra demasiado ambigua o idioma no soportado.

No usar el género musical, reputación del artista, título del álbum ni conocimiento externo al texto para anotar.

### 14.4. Métricas mínimas

Reportar:

- Matriz de confusión.
- Precision, recall y F1 por clase.
- Macro-F1.
- Accuracy como métrica secundaria.
- Cohen’s kappa entre anotadores, si hay dos anotadores.

No afirmar que el modelo es “state of the art para letras” sin evidencia de validación específica en el corpus del proyecto.

## 15. Agregaciones y análisis estadístico

### 15.1. Agregación a nivel canción

La unidad analítica primaria es la canción. Cada canción aporta un único `sentiment_score` agregado desde sus chunks.

### 15.2. Agregación por álbum

Para cada artista y álbum, calcular:

- Número de canciones (`song_count`).
- Media de score.
- Mediana de score.
- Desviación estándar.
- Intervalo de confianza bootstrap de la media.

Excluir álbumes con menos de `min_songs_per_album` canciones elegibles.

### 15.3. Agregación por año

Para cada artista y año, calcular las mismas métricas. Excluir años con menos de `min_songs_per_year` canciones elegibles de gráficos inferenciales, aunque se pueden mostrar como puntos transparentes con etiqueta de muestra pequeña.

### 15.4. Tendencia temporal

Ajustar, como análisis exploratorio:

\[
sentiment\_score_i = \beta_0 + \beta_1 \cdot year_i + \epsilon_i
\]

Interpretación:

- Si `beta_1 > 0`, el score tiende a hacerse más positivo con el tiempo.
- Si `beta_1 < 0`, el score tiende a hacerse más negativo con el tiempo.

Reportar siempre:

- Pendiente.
- Intervalo de confianza.
- p-value.
- R².
- Número de canciones.

No afirmar causalidad. Usar la frase “asociación temporal estimada” en lugar de “la banda se volvió definitivamente más alegre/deprimente”.

### 15.5. Comparación entre bandas

Para comparar dos artistas en el mismo periodo:

1. Filtrar a la intersección temporal definida por el usuario.
2. Reportar número de canciones por artista.
3. Reportar media, mediana y distribución.
4. Usar bootstrap para el intervalo de confianza de la diferencia de medias.
5. Complementar con tamaño de efecto (por ejemplo, Cohen’s d o Cliff’s delta).
6. Aplicar corrección de pruebas múltiples si se hacen muchas comparaciones de periodos o álbumes.

## 16. Visualizaciones requeridas

### 16.1. Línea temporal anual

- Eje X: año.
- Eje Y: media anual de `sentiment_score`.
- Una línea por artista.
- Banda de intervalo de confianza cuando el tamaño muestral lo permita.
- Línea horizontal en cero.
- Mostrar `n` anual en tooltip, etiqueta o tabla complementaria.

### 16.2. Comparación de artistas

- Usar violin plot o boxplot por artista para el periodo seleccionado.
- Superponer puntos de canciones con jitter si no dificulta lectura.
- Mostrar media, mediana y tamaño de muestra.

### 16.3. Sentimiento por álbum

- Barras ordenadas por fecha de lanzamiento.
- Error bars con intervalos bootstrap.
- Excluir álbumes bajo el umbral mínimo y mostrar el motivo en tabla metodológica.

### 16.4. Distribución de scores

- Histograma o KDE por artista.
- Mismo rango X en todos los artistas comparados.
- Línea vertical en cero.

### 16.5. Heatmap álbum-año

- Filas: álbumes.
- Columnas: métricas de sentimiento, opcionalmente emociones en fase posterior.
- Incluir `song_count` como anotación.

### 16.6. Principios visuales

1. No usar escalas distintas entre gráficos comparables.
2. No ocultar tamaños de muestra.
3. Distinguir valores observados de tendencias ajustadas.
4. Usar paletas accesibles para daltonismo.
5. Exportar PNG a 300 DPI y, cuando aplique, HTML interactivo con Plotly.

## 17. Flujo de ejecución

### 17.1. Inicialización

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example .env
```

Completar manualmente `GENIUS_ACCESS_TOKEN` en `.env`.

### 17.2. Comandos objetivo

Implementar CLI con comandos equivalentes a:

```bash
python -m lyrics_sentiment.cli fetch --artists config/artists.yaml
python -m lyrics_sentiment.cli validate-data
python -m lyrics_sentiment.cli process
python -m lyrics_sentiment.cli analyze-sentiment
python -m lyrics_sentiment.cli aggregate
python -m lyrics_sentiment.cli visualize
python -m lyrics_sentiment.cli run-all
```

### 17.3. Reglas del comando `run-all`

El comando debe ejecutar en este orden:

1. Validar configuración.
2. Obtener datos faltantes usando cache.
3. Validar calidad de adquisición.
4. Limpiar y deduplicar.
5. Segmentar letras.
6. Ejecutar inferencia.
7. Agregar resultados.
8. Ejecutar análisis estadístico.
9. Generar tablas y figuras.
10. Guardar un manifiesto de ejecución con versiones y timestamp.

Si falla un paso crítico, detener el pipeline y devolver un error explícito. No continuar silenciosamente con datasets parcialmente corruptos.

## 18. Testing y calidad de código

### 18.1. Tests obligatorios

Implementar tests para:

- Carga y validación de YAML.
- Ausencia de token Genius.
- Manejo de respuesta vacía o error de API.
- Limpieza de etiquetas `[Verse]`, `[Chorus]`, etc.
- Conservación de negaciones durante limpieza.
- Chunking de textos de menos, igual y más de 512 tokens.
- Ponderación correcta por número de tokens.
- Mapeo correcto de etiquetas del modelo.
- Rango del `sentiment_score`.
- Exclusión de álbumes y años con pocas canciones.

### 18.2. Controles antes de commit

```bash
ruff check .
ruff format --check .
pytest
mypy src
```

Configurar `pre-commit` para ejecutar al menos Ruff y tests rápidos.

## 19. Gestión de copyright y publicación

### 19.1. No publicar

No subir al repositorio público:

- Letras completas.
- Respuestas crudas de APIs que incluyan letras completas.
- Tokens, claves o archivos `.env`.
- Cache de peticiones.

### 19.2. Sí publicar

Se puede publicar, sujeto a términos de fuentes:

- Código fuente.
- Configuraciones sin secretos.
- Metadata mínima permitida.
- IDs internos, URLs de fuente y hashes de letras si se necesita trazabilidad sin reproducir texto.
- Scores agregados por canción, álbum, año y artista.
- Figuras, tablas y notebooks sin letras completas.
- Protocolo de validación y resultados agregados.

### 19.3. `.gitignore` mínimo

```gitignore
.env
.venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
data/raw/
data/interim/
data/cache/
*.ipynb_checkpoints
```

## 20. Dashboard opcional con Streamlit

El dashboard se implementa únicamente después de completar y validar el pipeline offline.

Funciones mínimas:

1. Selector de artistas.
2. Selector de rango de años.
3. Selector de álbumes.
4. Línea temporal comparativa.
5. Distribución de scores.
6. Tabla de estadísticas con tamaño muestral e intervalos.
7. Exportación de resultados agregados a CSV.

El dashboard no debe solicitar ni mostrar letras completas.

## 21. Entregables para portfolio

El repositorio público debe incluir:

1. README claro y visual.
2. Arquitectura del pipeline.
3. Instrucciones de reproducción.
4. Decisiones metodológicas y limitaciones.
5. Datos de ejemplo o resultados agregados permitidos.
6. Figuras de comparación temporal y por álbum.
7. Notebook de análisis reproducible.
8. Tests automatizados.
9. Dashboard opcional desplegable en una plataforma gratuita compatible.

## 22. Limitaciones que deben declararse

1. Las letras pueden estar incompletas, mal atribuidas o variar entre fuentes.
2. Genius y otras fuentes pueden cambiar sus términos, cobertura o comportamiento de API.
3. Un modelo entrenado en tweets no está entrenado específicamente en letras musicales.
4. Letras metafóricas, ironía y narradores ficticios dificultan la inferencia de sentimiento.
5. Repeticiones de estribillos pueden sobrerrepresentar una emoción.
6. La valencia textual no equivale a la emoción musical total: melodía, armonía, tempo e interpretación no están incluidos.
7. Fechas, álbumes y versiones pueden tener ambigüedad, especialmente en reediciones, recopilatorios, directos y remasters.
8. Comparaciones entre artistas requieren controlar periodo temporal y tamaño de muestra.
9. Las asociaciones temporales no prueban un cambio causal en la intención artística de una banda.

## 23. Roadmap de implementación

### Sprint 1: Base del proyecto

Criterios de aceptación:

- Repositorio creado.
- Entorno Python configurado.
- `requirements.txt` y lockfile generados.
- `.env.example`, `.gitignore`, YAMLs y estructura de carpetas listos.
- Ruff y pytest ejecutándose.

### Sprint 2: Adquisición y calidad de datos

Criterios de aceptación:

- `LyricsFetcher` funcional.
- Cache y reintentos implementados.
- Obtención de The Cure y The Smiths.
- Reporte de cobertura generado.
- No se publican letras ni tokens.

### Sprint 3: Limpieza, deduplicación y chunking

Criterios de aceptación:

- Letras limpias localmente.
- Duplicados identificados.
- Versiones no canónicas marcadas.
- Chunking validado por tests.

### Sprint 4: Inferencia y validación

Criterios de aceptación:

- Modelo RoBERTa cargado correctamente.
- Inference por batch y device configurables.
- Scores guardados en Parquet.
- Muestra de validación manual definida.
- Métricas de desempeño calculadas.

### Sprint 5: Agregación y análisis temporal

Criterios de aceptación:

- Resúmenes por canción, álbum y año.
- Intervalos bootstrap.
- Comparación The Cure vs The Smiths en un periodo común.
- Regresión temporal exploratoria y tabla de resultados.

### Sprint 6: Comunicación y portfolio

Criterios de aceptación:

- Gráficos finales exportados.
- README completo.
- Limitaciones visibles.
- Notebook final reproducible.
- Dashboard opcional funcional.

## 24. Prompt de continuación para otro LLM

```text
Estoy construyendo un proyecto de Data Science llamado "lyrics-sentiment-analysis". Debes seguir estrictamente el documento `plan_proyecto_analisis_sentimiento_letras_actualizado.md` como fuente de verdad.

Objetivo: obtener letras de canciones mediante Genius API con `lyricsgenius`, analizar sentimiento por canción, álbum, año y artista, y comparar artistas como The Cure vs The Smiths en el mismo periodo temporal.

Restricciones obligatorias:
- Python >=3.11,<3.13.
- Stack gratuito y open source.
- No publicar letras completas, credenciales, cache ni archivos raw.
- Usar `cardiffnlp/twitter-roberta-base-sentiment-latest` como baseline principal.
- Calcular `sentiment_score = positive - negative`.
- Si las letras exceden el límite de tokens, usar chunking semántico y agregación ponderada por token_count.
- Todos los parámetros deben venir de YAML; no hardcodear umbrales analíticos.
- Validar el orden de etiquetas con `model.config.id2label`.
- Mantener los nombres de columnas, módulos y funciones públicas definidos en el documento.
- Implementar tests antes de considerar una fase terminada.
- Toda conclusión debe informar tamaños muestrales, incertidumbre y limitaciones.

Primero revisa el estado actual del repositorio. Luego indica el sprint actual, los archivos que faltan y un plan concreto de cambios. No modifiques modelos, fuentes de datos ni arquitectura sin justificarlo explícitamente y actualizar README/CHANGELOG.
```
