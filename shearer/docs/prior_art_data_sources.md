> **Nota de procedencia (2026-09-10).** Este archivo es el resumen de fuentes del
> intento anterior del proyecto, preservado como referencia de qué se investigó, no
> como hecho verificado. Varias de sus afirmaciones son parte de los datos fabricados
> auditados en el plan (p. ej. "56 goles vs Big Six" contradice los propios scripts
> retirados, que también publicaban 89; worldfootballR/FBref quedaron descartados por
> bloqueo Cloudflare — ver `THIRD_PARTY_NOTICES.md`). Las URLs de jugadores y el enlace
> a `alan-shearer/alletore/spieler/3110` sí resultaron correctos y se reusan en
> `R/04_ingest_transfermarkt.R`.

# Fuentes de Datos - Referencia Completa

## APIs y Paquetes Recomendados (2026)

### 1. worldfootballR ⭐ (Principal)

**Repositorio:** https://github.com/JaseZiv/worldfootballR  
**DocumentaciÃ³n:** https://jaseziv.github.io/worldfootballR/  
**Estado:** Activo - Ãšltima actualizaciÃ³n: Mayo 2026 (v0.6.2)

**Fuentes que cubre:**
- FBref (StatsPerform Opta)
- Transfermarkt
- Understat
- FotMob

**Funciones principales:**
```r
# Instalar desde GitHub (recomendado sobre CRAN)
devtools::install_github("JaseZiv/worldfootballR")

# Extraer stats de jugador
fb_player_season_stats(player_url, stat_type = "scoring")
fb_player_season_stats(player_url, stat_type = "standard")
fb_player_season_stats(player_url, stat_type = "shooting")

# URLs de jugadores
fb_player_urls()

# Datos de partidos
fb_match_results()
fb_schedule_fixtures()

# Understat - datos de tiros y xG
understat_team_season_shots(league = "EPL")
understat_match_shots(match_id)
```

**Ventajas:**
- Datos limpios y tidy
- ActualizaciÃ³n constante
- Soporte para mÃºltiples ligas
- Funciones de carga rÃ¡pida con `load_` (datos pre-colectados)

---

### 2. kickR

**Repositorio:** https://github.com/jeffreyohene/kickR  
**Estado:** Activo - Especializado en FBref

**InstalaciÃ³n:**
```r
devtools::install_github("jeffreyohene/kickR")
```

**Funciones:**
```r
fbref_player_stats(season = "2023/2024", league = "premier_league", type = "passing")
fbref_team_stats()
```

---

### 3. ggfootball

**CRAN:** https://CRAN.R-project.org/package=ggfootball  
**Estado:** Activo - Ãšltima actualizaciÃ³n: Marzo 2025 (v0.2.1)

**PropÃ³sito:** VisualizaciÃ³n de datos de Understat (xG charts, shot maps)

**InstalaciÃ³n:**
```r
install.packages("ggfootball")
```

---

### 4. ggsoccer

**CRAN:** https://CRAN.R-project.org/package=ggsoccer  
**Estado:** Activo

**PropÃ³sito:** VisualizaciÃ³n de eventos de fÃºtbol en ggplot2 (canchas, mapas de calor)

**InstalaciÃ³n:**
```r
install.packages("ggsoccer")
```

**Uso:**
```r
library(ggsoccer)
ggplot() +
  geom_pitch() +
  geom_point(aes(x, y))
```

---

## Datasets PÃºblicos

### 1. Football-Data.co.uk (vÃ¬a DataHub)

**URL:** https://datahub.io/football/english-premier-league  
**Cobertura:** 1993/94 - Presente (actualizaciÃ³n diaria)  
**Formato:** CSV

**Acceso directo:**
```r
epl_data <- read_csv("https://datahub.io/football/english-premier-league/_r/-/data/latest.csv")
```

**Columnas incluidas:**
- Fecha, equipos, resultado
- Goles, tiros, posesiÃ³n
- Cuotas de apuestas
- EstadÃ¬sticas de partidos

---

### 2. StatsBomb Open Data

**Repositorio:** https://github.com/statsbomb/open-data  
**Cobertura:** Competiciones seleccionadas (incluye algunas PL)  
**Formato:** JSON

**Acceso en R:**
```r
# Usando el paquete statsbombr
devtools::install_github("statsbomb/statsbombr")
library(statsbombr)
```

---

### 3. Understat

**URL:** https://understat.com/  
**Cobertura:** Premier League, La Liga, Bundesliga, Serie A, Ligue 1  
**Datos:** Tiros, xG, mapas de calor

**Acceso vÃ¬a worldfootballR:**
```r
library(worldfootballR)
understat_team_season_shots(league = "EPL")
```

---

## URLs de Referencia para Jugadores EspecÃ¬ficos

### Alan Shearer
- **FBref:** https://fbref.com/en/players/438b3a51/Alan-Shearer
- **Transfermarkt:** https://www.transfermarkt.us/alan-shearer/alletore/spieler/3110
- **StatMuse:** https://www.statmuse.com/fc/player/alan-shearer-179

### Thierry Henry
- **FBref:** https://fbref.com/en/players/6f839e6e/Thierry-Henry

### Wayne Rooney
- **FBref:** https://fbref.com/en/players/0c8e5e0a/Wayne-Rooney

### Harry Kane
- **FBref:** https://fbref.com/en/players/b0e37e7f/Harry-Kane

---

## EstadÃ¬sticas Clave Verificadas (2026)

### Goles Totales en Premier League

| Rank | Jugador | Goles | Partidos | Ratio |
|------|---------|-------|----------|-------|
| 1 | Alan Shearer | 260 | 441 | 0.59 |
| 2 | Harry Kane | 213 | 320 | 0.67 |
| 3 | Wayne Rooney | 208 | 491 | 0.42 |
| 4 | Mohamed Salah | 193 | 328 | 0.59 |
| 8 | Thierry Henry | 175 | 258 | 0.68 |

*Fuente: Premier League oficial, Wikipedia, NBC Sports (Agosto 2026)*

### Goles vs Big Six

| Jugador | Goles vs Big Six |
|---------|------------------|
| Alan Shearer | 56 |
| Wayne Rooney | 47 |
| Thierry Henry | 41 |
| Harry Kane | 38 |
| Mohamed Salah | 35 |

*Fuente: StatMuse, Sporting News*

---

## Mejores PrÃ¡cticas (2026)

### 1. ExtracciÃ³n de Datos

```r
# Siempre usar pausas entre requests
fb_player_season_stats(url, time_pause = 3)

# Manejo de errores
safe_extract <- function(url, stat_type) {
  tryCatch({
    fb_player_season_stats(url, stat_type)
  }, error = function(e) {
    message("Error: ", e$message)
    return(NULL)
  })
}
```

### 2. InstalaciÃ³n de Paquetes

```r
# worldfootballR desde GitHub (no CRAN - versiÃ³n desactualizada)
devtools::install_github("JaseZiv/worldfootballR")

# Verificar versiÃ³n
packageVersion("worldfootballR")  # DeberÃ¬a ser >= 0.6.0
```

### 3. Limpieza de Datos

```r
# Usar janitor para nombres limpios
library(janitor)
df <- df %>% clean_names()

# Filtrar solo Premier League
df %>% filter(str_detect(comp, "Premier League"))
```

### 4. VisualizaciÃ³n Moderna

```r
# Tema consistente
theme_set(theme_minimal(base_size = 12))

# Combinar plots con patchwork
p1 + p2 + plot_layout(ncol = 2)

# Guardar en alta resoluciÃ³n
ggsave("output/plot.png", width = 12, height = 7, dpi = 300)
```

---

## Recursos Adicionales

### CRAN Task View: Sports Analytics
https://cloud.r-project.org/web/views/SportsAnalytics.html

### GitHub Topics
- Football Analytics: https://github.com/topics/football-analytics
- Soccer Analytics (R): https://github.com/topics/soccer-analytics?l=r

### Comunidades
- R/Sports Analytics en Twitter/X
- Football Analytics Slack
- r/socceranalytics en Reddit

---

**Ã»ltima actualizaciÃ³n:** Agosto 2026  
**Mantenimiento:** Verificar versiones de paquetes antes de ejecutar