---
title: English
---

# Dengue and Zika surveillance in Argentina

<p class="lede">An open, reproducible analytics pipeline over Argentina's national Dengue and Zika notifications (SNVS 2.0): dbt models on Databricks, tested and documented, feeding a Power BI report and this static site. The pages are in Spanish because the audience is Argentine public health teams.</p>

- **[Situación actual](./)**: cases and rate per 100,000 by department.
- **[Corredor endémico](./corredor)**: endemic corridor per province, comparing each week with the same weeks of the previous 5 years.
- **[Brotes](./brotes)**: outbreak episodes (runs of weeks above the corridor's 75th percentile) and their attack rates.
- **[Hallazgos](./hallazgos)**: six surveillance questions answered in SQL. Highlights: the north-to-south spread hypothesis does not hold; a growth signal alone is a weak outbreak alert but gives weeks of lead time when it hits; hot spots move from season to season; late revisions are rare but can double a count.
- **[Metodología](./metodologia)**: sources, definitions and limitations.

Data: Ministry of Health (SNVS 2.0) and RENAPER, CC-BY 4.0. Code: MIT. Source and data lineage: [GitHub](https://github.com/datakrdo/portfolio/tree/main/vigilancia-ar) and the [dbt docs](https://datakrdo.github.io/portfolio/dbt/).
