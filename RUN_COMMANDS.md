# Полный набор команд для перезапуска экспериментов статьи

Все команды выполняются из корня репозитория. 

## Схема повторов

| Масштаб | `APP2_REPEATS_OVERRIDE` |
|---------|-------------------------|
| small   | 10                      |
| medium  | 5                       |
| large   | 3                       |

Это переменная окружения, которая переопределяет `defaults.repeats` из YAML.
В PostgreSQL-режиме повторы применяются к каждому инструменту через
`tech.validation_run`. В Spark-режиме повторы применяются циклом в
`_run_spark_validation`. Меньше повторов на больших датасетах — компромисс
между статистической оценкой и временем выполнения.

## Один скрипт-обёртка (рекомендуется)

```bash
bash scripts/run_paper_experiments.sh small  all      
bash scripts/run_paper_experiments.sh medium all     
bash scripts/run_paper_experiments.sh large  all     
```

Для запуска только PostgreSQL или только Spark:
```bash
bash scripts/run_paper_experiments.sh small  pg
bash scripts/run_paper_experiments.sh small  spark
```

---

## Полный список команд

### Small (10 повторов)

```bash
export APP2_REPEATS_OVERRIDE=10
SCALE_DIR=input/scaled_datasets/scaled_small_20260429_204117
SCALE_BASE=input/scaled_datasets

# --- PostgreSQL ---
# 1. Все инструменты сразу 
python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_all.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_all_tools \
  --logs-dir   logs/paper/small/pg_all_tools \
  --persist-mutation-report --persist-tool-reports

# 2. Универсальные 
python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_universal_gx.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_universal_gx \
  --logs-dir   logs/paper/small/pg_universal_gx \
  --persist-mutation-report --persist-tool-reports

python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_universal_soda.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_universal_soda \
  --logs-dir   logs/paper/small/pg_universal_soda \
  --persist-mutation-report --persist-tool-reports

python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_universal_dbt.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_universal_dbt \
  --logs-dir   logs/paper/small/pg_universal_dbt \
  --persist-mutation-report --persist-tool-reports

# 3. Этапные 
python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_etap_soda_sql_soda.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_etap_soda_sql_soda \
  --logs-dir   logs/paper/small/pg_etap_soda_sql_soda \
  --persist-mutation-report --persist-tool-reports

python scripts/run_manual_experiments.py --engine postgres --start-temp-db \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_pg_etap_soda_sql_dbt.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/pg_etap_soda_sql_dbt \
  --logs-dir   logs/paper/small/pg_etap_soda_sql_dbt \
  --persist-mutation-report --persist-tool-reports

# --- Spark ---
python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_all.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_all_tools \
  --logs-dir   logs/paper/small/spark_all_tools

python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_universal_gx.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_universal_gx \
  --logs-dir   logs/paper/small/spark_universal_gx

python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_universal_soda.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_universal_soda \
  --logs-dir   logs/paper/small/spark_universal_soda

python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_universal_deequ.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_universal_deequ \
  --logs-dir   logs/paper/small/spark_universal_deequ

python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_etap_soda_sql_soda.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_etap_soda_sql_soda \
  --logs-dir   logs/paper/small/spark_etap_soda_sql_soda

python scripts/run_manual_experiments.py --engine spark \
  --input-dir "$SCALE_BASE" --input-run-dir "$SCALE_DIR" \
  --tools-config config/tools_spark_etap_soda_deequ_soda.yml \
  --mutation-config config/mutation_safe.yml \
  --output-dir output/paper/small/spark_etap_soda_deequ_soda \
  --logs-dir   logs/paper/small/spark_etap_soda_deequ_soda
```

### Medium (5 повторов)

Те же команды, что и для small, но с заменой:
- `APP2_REPEATS_OVERRIDE=5`
- `SCALE_DIR=input/scaled_datasets/scaled_medium_20260429_204146`
- `output/paper/small/...` → `output/paper/medium/...`
- `logs/paper/small/...` → `logs/paper/medium/...`

### Large (3 повтора)

Аналогично, но:
- `APP2_REPEATS_OVERRIDE=3`
- `SCALE_DIR=input/scaled_datasets/scaled_large_20260429_204207`
- Замена `small` → `large` в путях.

---

## Что получится в результате

В директории `output/paper/<scale>/<strategy>/` появятся:

- `validation_summary_<timestamp>.csv` — основной CSV для агрегации 
  Поля: `stage`, `layer`, `tool`, `kind`, `runs`, `checks_total`, `checks_failed`,
  `avg_duration_ms`, `std_duration_ms`, `avg_cpu_percent`, `avg_rss_kb`, и др.
- Для Spark дополнительно `spark_summary_<timestamp>.csv`

Параллельно в `logs/paper/<scale>/<strategy>/` будут:
- `db_export_<timestamp>.log` — выгрузка `tech.etl_load_audit`,
  `tech.etl_batch_status`, `tech.validation_run`.
- `mutation_reports/experiment_<name>_<ts>.html` — HTML-отчёт мутационного эксперимента.
- `etl_stage_reports/<tool>/...json` — детальные JSON-отчёты каждого инструмента.




