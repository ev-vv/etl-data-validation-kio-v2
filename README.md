# ETL Data Validation

Воспроизводимый исследовательский стенд для статьи:
**«Экспериментальное сравнение средств валидации данных в ETL-пайплайнах»** (KIO, 2026).

Стенд реализует ETL-пайплайн на футбольных данных (`football-data.org`) в двух
вычислительных контурах и сравнивает средства валидации данных — Great Expectations,
SodaCL, dbt tests, SQL-проверки и Deequ — по метрикам обнаружения нарушений
и эксплуатационной стоимости.

- **PostgreSQL-контур:** `STG (raw JSON) → DDS (dim/fact) → MART (views)`.
- **S3-Spark-контур:** PySpark `local[*]` + Spark SQL/Catalyst поверх Parquet-таблиц
  в MinIO через S3A; в этом контуре отдельный MART не материализуется,
  L-этап проверяется на тех же Parquet-таблицах.

В стенде поддерживается контролируемая инъекция дефектов на этапах STG/DDS/Spark
и регистрация всех прогонов в технической схеме `tech` (audit, batch status,
validation runs).

---

## Содержание

1. [Авторы](#авторы)
2. [Состав репозитория](#состав-репозитория)
3. [Quick start — установка с нуля](#quick-start--установка-с-нуля)
4. [Демонстрационный прогон (одна таблица статьи)](#демонстрационный-прогон-одна-таблица-статьи)
5. [Полное воспроизведение результатов статьи](#полное-воспроизведение-результатов-статьи)
6. [Структура проекта](#структура-проекта)
7. [Используемые наборы данных](#используемые-наборы-данных)
8. [Лицензия и условия использования данных](#лицензия-и-условия-использования-данных)

---

## Авторы

- **Метёлкин Максим Александрович** — бакалавр Высшей школы программной
  инженерии Института компьютерных технологий и кибербезопасности, СПбПУ.
- **Пархоменко Владимир Андреевич** — научный руководитель,
  старший преподаватель Высшей школы программной инженерии Института компьютерных
  технологий и кибербезопасности, СПбПУ.
- **Евсеева Влада Владимировна** — бакалавр Высшей школы программной инженерии Института
  компьютерных технологий и кибербезопасности, СПбПУ.

---

## Состав репозитория

| Раздел | Содержимое |
|---|---|
| `src/app2/` | Python-приложение стенда: загрузчики STG, ETL для DDS, нативные валидаторы STG/DDS, раннеры GX / SodaCL / dbt / SQL / Deequ (PostgreSQL и Spark), мутаторы |
| `scripts/` | Точки входа: `run_manual_experiments.py`, `run_paper_experiments.sh`, `start_temp_db.py`, `generate_scaled_dataset.py`, `export_input_from_db.py` |
| `sql/initdb/` | Инициализация схем PostgreSQL: `stg`, `dds`, `mart`, `tech` |
| `config/` | YAML-профили инструментов (`tools_pg_*.yml`, `tools_spark_*.yml`) и конфигурация мутаций (`mutation_safe.yml`), `scale_presets.yml` |
| `input/` | Входные данные: baseline raw-выгрузка и масштабированный small-датасет |
| `output/paper/` | Агрегированные CSV-результаты прогонов, использованные в статье |
| `docker-compose.experiments.yml` | Контейнер PostgreSQL 16 для экспериментов |
| `requirements.experiments.txt` | Python-зависимости |
| `RUN_COMMANDS.md` | Полный набор команд для воспроизведения всех экспериментов статьи |

---

## Manual run

### Требования

- **Python 3.12+**
- **Docker Engine** или **Docker Desktop** (для временного PostgreSQL).
- **Apache Spark 3.5+** в `PATH` (нужен только для Spark-блока).
- Git.

### Шаги

```bash
# 1. Клонировать репозиторий
git clone https://github.com/ev-vv/etl-data-validation-kio-v2.git etl-data-validation-kio-v2
cd etl-data-validation-kio-v2

# 2. Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate            # Linux / macOS
# для Windows PowerShell:  .\venv\Scripts\Activate.ps1

# 3. Установить зависимости
pip install --upgrade pip
pip install -r requirements.experiments.txt

# 4. Проверить Docker
docker --version
docker compose version
```

Параметры подключения по умолчанию (используются, если нет `.env`):

```text
POSTGRES_DB=vkr_data
POSTGRES_USER=admin
POSTGRES_PASSWORD=pass
POSTGRES_HOST=localhost
POSTGRES_PORT=55432
```

Временный контейнер PostgreSQL разворачивается автоматически флагом
`--start-temp-db` и инициализируется скриптами из `sql/initdb/`.

---

## Демонстрационный прогон (одна таблица статьи)

Этот прогон занимает **~5–10 минут** и воспроизводит **Таблицу 3** статьи —
сравнение GX, SodaCL, dbt tests и SQL по этапам E / T / L в PostgreSQL-контуре
на масштабе `small`.

```bash
# Кол-во повторов для small
export APP2_REPEATS_OVERRIDE=10

python scripts/run_manual_experiments.py \
    --engine postgres \
    --start-temp-db \
    --input-dir input/scaled_datasets \
    --input-run-dir input/scaled_datasets/scaled_small_20260429_204117 \
    --tools-config config/tools_pg_all.yml \
    --mutation-config config/mutation_safe.yml \
    --output-dir output/demo/pg_all_tools \
    --logs-dir   logs/demo/pg_all_tools \
    --persist-mutation-report \
    --persist-tool-reports
```

После завершения:

```bash
# Сводный CSV 
ls output/demo/pg_all_tools/
cat output/demo/pg_all_tools/validation_summary_*.csv | column -t -s,

# Остановить временный PostgreSQL
docker compose -f docker-compose.experiments.yml down -v
```

Поля CSV: `stage`, `layer`, `tool`, `kind`, `runs`, `checks_total`, `checks_failed`,
`avg_duration_ms`, `std_duration_ms`, `avg_cpu_percent`, `std_cpu_percent`,
`avg_rss_kb`, `std_rss_kb`. Для Spark-прогонов дополнительно сохраняются
`avg_spark_duration_ms` / `std_spark_duration_ms` 

---

## Полное воспроизведение результатов статьи

Полный набор прогонов запускается одной обёрткой:

```bash
bash scripts/run_paper_experiments.sh <SCALE> <BLOCK>
```

| Параметр | Допустимые значения | Описание |
|---|---|---|
| `SCALE` | `small`, `medium`, `large` | Масштаб датасета |
| `BLOCK` | `pg`, `spark`, `all` | Какой контур запускать |

Повторы устанавливаются автоматически: `small=10`, `medium=5`, `large=3`.

```bash
# Полный прогон small 
bash scripts/run_paper_experiments.sh small all

# Только PostgreSQL-контур
bash scripts/run_paper_experiments.sh small pg

# Только Spark-контур
bash scripts/run_paper_experiments.sh small spark
```

Стратегии, прогоняемые скриптом:

| Контур | Универсальные | Этапные |
|---|---|---|
| **PostgreSQL** | GX, SodaCL, dbt | SodaCL + SQL + SodaCL, SodaCL + SQL + dbt |
| **Spark** | GX, SodaCL, Deequ | SodaCL + SQL + SodaCL, SodaCL + Deequ + SodaCL |

Результаты:

- `output/paper/<scale>/<strategy>/validation_summary_*.csv` — основной CSV
  с агрегированными метриками (вход для таблиц и графиков статьи).
- `output/paper/<scale>/<strategy>/spark_summary_*.csv` — дополнительный CSV
  для Spark-прогонов (включая `raw Spark time`).
- `logs/paper/<scale>/<strategy>/` — детальные артефакты прогона:
  HTML-отчёты мутационного эксперимента, JSON-отчёты инструментов,
  дампы `tech.etl_load_audit`, `tech.etl_batch_status`, `tech.validation_run`.

Подробные команды для каждой комбинации см. в [`RUN_COMMANDS.md`](RUN_COMMANDS.md).

---

## Структура проекта

```
.
├── config/
│   ├── tools_pg_all.yml                    # все 4 инструмента в Postgres 
│   ├── tools_pg_universal_gx.yml           # унив. стратегия GX
│   ├── tools_pg_universal_soda.yml         # унив. стратегия SodaCL
│   ├── tools_pg_universal_dbt.yml          # унив. стратегия dbt
│   ├── tools_pg_etap_soda_sql_soda.yml     # этапная Soda+SQL+Soda
│   ├── tools_pg_etap_soda_sql_dbt.yml      # этапная Soda+SQL+dbt
│   ├── tools_spark_all.yml                 # все 4 инструмента в Spark
│   ├── tools_spark_universal_gx.yml
│   ├── tools_spark_universal_soda.yml
│   ├── tools_spark_universal_deequ.yml
│   ├── tools_spark_etap_soda_sql_soda.yml
│   ├── tools_spark_etap_soda_deequ_soda.yml
│   ├── mutation_safe.yml                  
│   └── scale_presets.yml                   # размеры датасетов small / medium / large
│
├── scripts/
│   ├── run_manual_experiments.py           # одиночный прогон (PG или Spark)
│   ├── run_paper_experiments.sh            # полный набор для статьи
│   ├── start_temp_db.py                    # временный PostgreSQL в Docker
│   ├── generate_scaled_dataset.py          # генерация small / medium / large
│   └── export_input_from_db.py             # выгрузка raw-данных из БД в JSON-payload
│
├── src/app2/
│   ├── core/                               # настройки
│   ├── db/                                 # SQLAlchemy-коннектор, audit, batch, validation_run
│   ├── loaders/                            # raw_staging.py, batch_staging.py (STG)
│   ├── dds/                                # ETL для DDS (PostgreSQL)
│   ├── spark/                              # Spark-сессия и батчевый загрузчик в Parquet
│   ├── mutators/                           # контролируемая инъекция дефектов (STG/DDS/Spark)
│   ├── validators/                         # нативные валидаторы STG/DDS (схема, полнота,
│   │                                       # уникальность, согласованность, целостность, бизнес-правила)
│   ├── etl_validation/                     # раннеры GX / SodaCL / dbt / SQL / Deequ (PG и Spark)
│   ├── post_validation/                    # пост-валидация (GX, SodaCL, dbt) поверх MART
│   └── experiments/                        # оркестрация мутационных экспериментов
│
├── sql/initdb/
│   ├── stg_tables.sql                      # stg.raw_football_api
│   ├── dds_tables.sql                      # dim_*/fact_* + check-constraints
│   ├── mart_views.sql                      # v_competition_season_kpi, v_team_season_results
│   ├── tech_tables.sql                     # etl_load_audit, etl_batch_status, validation_run
│   └── zz_add_batch_id.sql
│
├── input/
│   ├── raw_football_api/                   # baseline raw-выгрузка из API
│   └── scaled_datasets/scaled_small_*/     # small (≈1.8k матчей)
│
├── output/paper/<scale>/<strategy>/        # агрегированные CSV 
│
├── docker-compose.experiments.yml
├── requirements.experiments.txt
├── RUN_COMMANDS.md                         # развёрнутые команды воспроизведения
└── README.md
```

---

## Используемые наборы данных

Демонстрационный домен — футбольная статистика из публичного API
[football-data.org](https://www.football-data.org/) (соревнования, сезоны, команды,
матчи, турнирные таблицы, бомбардиры).

Масштабированные датасеты формируются генератором путём детерминированного
повторения сущностей со смещением идентификаторов:

| Масштаб | Соревнований | Команд | Матчей | Сезонов | Областей |
|---|---|---|---|---|---|
| small | 3 | 8 на соревнование | ~1 800 | 2023–2024 | 5 |
| medium | 10 | 20 на соревнование | ~30 000 | 2022–2025 | 15 |
| large | 30 | 50 на соревнование | ~1 200 000 | 2015–2025 | 30 |

---

## Лицензия и условия использования данных

Исходный код проекта распространяется под **лицензией MIT** (см. файл `LICENSE`).

Футбольная статистика получена через API [football-data.org](https://www.football-data.org/)
и регулируется условиями этого сервиса (General Terms and Conditions от 01.06.2018),
в том числе:

- регистрация и валидный API-ключ;
- соблюдение Fair Use Policy и ограничений тарифного плана;
- обязательная атрибуция: *«Football data provided by the Football-Data.org API»*;
- ограничения по интеллектуальной собственности на графические материалы
  (логотипы команд и т. п.).

Лицензия MIT распространяется **только на исходный код** и не распространяется
на футбольные данные, полученные через сторонний API.

Авторы предоставляют программное обеспечение «как есть», без каких-либо гарантий.
