set -euo pipefail

SCALE="${1:-small}"
BLOCK="${2:-all}"

declare -A INPUT_RUN_DIRS=(
    [small]="input/scaled_datasets/scaled_small_20260429_204117"
    [medium]="input/scaled_datasets/scaled_medium_20260429_204146"
    [large]="input/scaled_datasets/scaled_large_20260429_204207"
)

declare -A REPEATS_BY_SCALE=(
    [small]=10
    [medium]=5
    [large]=3
)

if [[ -z "${INPUT_RUN_DIRS[$SCALE]:-}" ]]; then
    echo "ERROR: unknown scale '$SCALE'. Allowed: small, medium, large." >&2
    exit 1
fi

INPUT_RUN_DIR="${INPUT_RUN_DIRS[$SCALE]}"
export APP2_REPEATS_OVERRIDE="${REPEATS_BY_SCALE[$SCALE]}"

if [[ ! -d "$INPUT_RUN_DIR" ]]; then
    echo "ERROR: input run directory not found: $INPUT_RUN_DIR" >&2
    exit 1
fi

OUT_BASE="output/paper/$SCALE"
LOG_BASE="logs/paper/$SCALE"
mkdir -p "$OUT_BASE" "$LOG_BASE"

echo "================================================================="
echo "Paper experiments: scale=$SCALE  block=$BLOCK  repeats=$APP2_REPEATS_OVERRIDE"
echo "Input run dir:    $INPUT_RUN_DIR"
echo "Output base:      $OUT_BASE"
echo "Logs base:        $LOG_BASE"
echo "================================================================="

run_pg_experiment() {
    local strategy="$1"
    local tools_cfg="$2"

    local out_dir="$OUT_BASE/pg_$strategy"
    local log_dir="$LOG_BASE/pg_$strategy"
    mkdir -p "$out_dir" "$log_dir"

    echo
    echo "[PG] strategy=$strategy  tools_cfg=$tools_cfg"
    python scripts/run_manual_experiments.py \
        --engine postgres \
        --start-temp-db \
        --input-dir "$(dirname "$INPUT_RUN_DIR")" \
        --input-run-dir "$INPUT_RUN_DIR" \
        --tools-config "$tools_cfg" \
        --mutation-config config/mutation_safe.yml \
        --output-dir "$out_dir" \
        --logs-dir "$log_dir" \
        --persist-mutation-report \
        --persist-tool-reports
}

run_spark_experiment() {
    local strategy="$1"
    local tools_cfg="$2"

    local out_dir="$OUT_BASE/spark_$strategy"
    local log_dir="$LOG_BASE/spark_$strategy"
    mkdir -p "$out_dir" "$log_dir"

    echo
    echo "[SPARK] strategy=$strategy  tools_cfg=$tools_cfg"
    python scripts/run_manual_experiments.py \
        --engine spark \
        --input-dir "$(dirname "$INPUT_RUN_DIR")" \
        --input-run-dir "$INPUT_RUN_DIR" \
        --tools-config "$tools_cfg" \
        --mutation-config config/mutation_safe.yml \
        --output-dir "$out_dir" \
        --logs-dir "$log_dir"
}

if [[ "$BLOCK" == "pg" || "$BLOCK" == "all" ]]; then
    run_pg_experiment "all_tools"        "config/tools_pg_all.yml"

    run_pg_experiment "universal_gx"     "config/tools_pg_universal_gx.yml"
    run_pg_experiment "universal_soda"   "config/tools_pg_universal_soda.yml"
    run_pg_experiment "universal_dbt"    "config/tools_pg_universal_dbt.yml"

    run_pg_experiment "etap_soda_sql_soda" "config/tools_pg_etap_soda_sql_soda.yml"
    run_pg_experiment "etap_soda_sql_dbt"  "config/tools_pg_etap_soda_sql_dbt.yml"
fi

if [[ "$BLOCK" == "spark" || "$BLOCK" == "all" ]]; then
    run_spark_experiment "all_tools"      "config/tools_spark_all.yml"

    run_spark_experiment "universal_gx"   "config/tools_spark_universal_gx.yml"
    run_spark_experiment "universal_soda" "config/tools_spark_universal_soda.yml"
    run_spark_experiment "universal_deequ" "config/tools_spark_universal_deequ.yml"

    run_spark_experiment "etap_soda_sql_soda"   "config/tools_spark_etap_soda_sql_soda.yml"
    run_spark_experiment "etap_soda_deequ_soda" "config/tools_spark_etap_soda_deequ_soda.yml"
fi

echo
echo "================================================================="
echo "DONE. Аггрегированные результаты см. в:"
echo "  $OUT_BASE/*/validation_summary_*.csv"
echo "================================================================="
