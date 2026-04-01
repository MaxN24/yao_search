#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run a grid sweep for Yao forward search and log outcomes.

Usage:
  scripts/run_yao_sweep.sh [options]

Options:
  --m-min INT                 Minimum m (default: 5)
  --m-max INT                 Maximum m (default: 8)
  --n-min INT                 Minimum subset size n (default: 3)
  --n-max INT                 Maximum subset size n (default: 5)
  --max-total-m INT           Hard cap for total poset size m (default: 16)
  --extra-min INT             Minimum extra elements (m-n). If set, uses m=n+extra.
  --extra-max INT             Maximum extra elements (m-n). Must be used with --extra-min.
  --i-mode all|half           Accepted for compatibility; the sweep follows the fixed plan below
  --max-cache-size BYTES      Forward cache size (default: 4294967296 / 4 GiB)
  --resume-csv PATH           Resume from existing CSV and skip finished (m,n,i), append new rows
  --progress-file PATH        Persistent index of computed (m,n,i) across runs
                             (default: logs/yao/yao_progress_index.csv)
  --stop-on-counterexample    Stop immediately when first counterexample is found
                             Note: Yao > classic is always treated as implementation_bug and stops.
  --classic-check             Enable --yao-classic-check (test only classic-1 and classic)
  --no-yao-prune              Enable --yao-disable-optimistic-prune
  --no-build                  Skip `cargo build --release`
  --bin PATH                  Binary path (default: ./target/release/selection_generator)
  -h, --help                  Show this help

Examples:
  scripts/run_yao_sweep.sh --m-min 5 --m-max 8 --n-min 3 --n-max 5
  scripts/run_yao_sweep.sh --n-min 5 --n-max 10 --extra-min 1 --extra-max 4 --i-mode half
  scripts/run_yao_sweep.sh --n-min 5 --n-max 10 --extra-min 1 --extra-max 4 --i-mode half --resume-csv logs/yao/yao_sweep_20260212_163523.csv
  scripts/run_yao_sweep.sh --m-min 6 --m-max 10 --n-min 4 --n-max 6 --i-mode half --stop-on-counterexample
EOF
}

m_min=5
m_max=8
n_min=3
n_max=5
max_total_m=16
extra_min=""
extra_max=""
i_mode="all"
max_cache_size=4294967296
resume_csv=""
log_root="logs/yao"
progress_file="${log_root}/yao_progress_index.csv"
stop_on_counterexample=0
classic_check=0
no_yao_prune=0
do_build=1
bin_path="./target/release/selection_generator"

# Sweep plan for the classic selection lines we care about.
# Fields: n, i_1based, classic_value, m_max
SWEEP_PLAN=(
  "3,2,3,3"
  "4,2,4,4"
  "5,2,6,7"
  "5,3,6,7"
  "6,2,7,8"
  "6,3,8,10"
  "7,2,8,9"
  "7,3,10,13"
  "7,4,10,13"
  "8,2,9,10"
  "8,3,11,14"
  "8,4,12,16"
  "9,2,11,13"
  "9,3,12,15"
  "9,4,14,16"
  "9,5,14,16"
  "10,2,12,14"
  "10,3,14,16"
  "10,4,15,16"
  "10,5,16,16"
  "11,2,13,15"
  "11,3,15,16"
  "11,4,17,16"
  "11,5,18,16"
  "11,6,18,16"
  "12,2,14,16"
  "12,3,17,16"
  "12,4,18,16"
  "12,5,19,16"
  "12,6,20,16"
  "13,2,15,16"
  "13,3,18,16"
  "13,4,20,16"
  "13,5,21,16"
  "13,6,22,16"
  "13,7,23,16"
  "14,2,16,16"
  "14,3,19,16"
  "14,4,21,16"
  "14,5,23,16"
  "14,6,24,16"
  "14,7,25,16"
  "15,2,17,16"
  "15,3,20,16"
  "15,4,23,16"
  "15,5,24,16"
  "15,6,26,16"
  "15,7,26,16"
  "15,8,27,16"
  "16,2,18,16"
  "16,3,21,16"
  "16,4,24,16"
  "16,5,26,16"
  "16,6,27,16"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --m-min)
      m_min="$2"
      shift 2
      ;;
    --m-max)
      m_max="$2"
      shift 2
      ;;
    --n-min)
      n_min="$2"
      shift 2
      ;;
    --n-max)
      n_max="$2"
      shift 2
      ;;
    --max-total-m)
      max_total_m="$2"
      shift 2
      ;;
    --extra-min)
      extra_min="$2"
      shift 2
      ;;
    --extra-max)
      extra_max="$2"
      shift 2
      ;;
    --i-mode)
      i_mode="$2"
      shift 2
      ;;
    --max-cache-size)
      max_cache_size="$2"
      shift 2
      ;;
    --resume-csv)
      resume_csv="$2"
      shift 2
      ;;
    --progress-file)
      progress_file="$2"
      shift 2
      ;;
    --stop-on-counterexample)
      stop_on_counterexample=1
      shift
      ;;
    --classic-check)
      classic_check=1
      shift
      ;;
    --no-yao-prune)
      no_yao_prune=1
      shift
      ;;
    --no-build)
      do_build=0
      shift
      ;;
    --bin)
      bin_path="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$i_mode" != "all" && "$i_mode" != "half" ]]; then
  echo "Invalid --i-mode: $i_mode (expected all|half)" >&2
  exit 1
fi

if (( m_min < 1 || m_max < 1 || n_min < 1 || n_max < 1 )); then
  echo "m and n bounds must be >= 1" >&2
  exit 1
fi

if (( max_total_m < 1 )); then
  echo "--max-total-m must be >= 1" >&2
  exit 1
fi

if (( m_min > m_max || n_min > n_max )); then
  echo "Invalid bounds: min cannot be greater than max" >&2
  exit 1
fi

if (( n_min > max_total_m )); then
  echo "Invalid bounds: n-min ($n_min) exceeds max total m ($max_total_m)" >&2
  exit 1
fi

if [[ -n "$extra_min" || -n "$extra_max" ]]; then
  if [[ -z "$extra_min" || -z "$extra_max" ]]; then
    echo "Both --extra-min and --extra-max must be set together" >&2
    exit 1
  fi
  if (( extra_min < 0 || extra_max < 0 )); then
    echo "extra bounds must be >= 0" >&2
    exit 1
  fi
  if (( extra_min > extra_max )); then
    echo "Invalid extra bounds: extra-min cannot be greater than extra-max" >&2
    exit 1
  fi
fi

mkdir -p "$log_root"
mkdir -p "$(dirname "$progress_file")"
timestamp="$(date +%Y%m%d_%H%M%S)"
run_id="${timestamp}_$$"
if [[ -n "$resume_csv" ]]; then
  csv_file="$resume_csv"
  if [[ ! -f "$csv_file" ]]; then
    echo "--resume-csv file does not exist: $csv_file" >&2
    exit 1
  fi
  log_file="${log_root}/yao_sweep_resume_${run_id}.log"
else
  csv_file="${log_root}/yao_sweep_${run_id}.csv"
  log_file="${log_root}/yao_sweep_${run_id}.log"
  echo "timestamp,m,n,i,yao_result,classic_known,status" > "$csv_file"
fi
counter_file="${log_root}/yao_counterexamples_${run_id}.txt"
bug_file="${log_root}/yao_implementation_bugs_${run_id}.txt"
progress_summary_file="${progress_file%.csv}.summary.txt"

if [[ ! -s "$csv_file" ]]; then
  echo "timestamp,m,n,i,yao_result,classic_known,status" > "$csv_file"
fi

if [[ ! -f "$progress_file" ]]; then
  echo "m,n,i,yao_result,classic_known,status,last_timestamp,last_csv" > "$progress_file"
fi

# Avoid concurrent writers against the same CSV (especially in resume mode).
if command -v flock >/dev/null 2>&1; then
  lock_target="$csv_file"
  lock_file="${lock_target}.lock"
  exec 9>"$lock_file"
  if ! flock -n 9; then
    echo "Another sweep process is already writing to $csv_file (lock: $lock_file)" >&2
    exit 1
  fi
fi

# Avoid concurrent writes against the shared progress index file.
if command -v flock >/dev/null 2>&1; then
  progress_lock_file="${progress_file}.lock"
  exec 8>"$progress_lock_file"
  flock 8
fi

{
  echo "Yao sweep started at $(date -Iseconds)"
  if [[ -n "$extra_min" ]]; then
    echo "extra range (m-n): [$extra_min, $extra_max]"
  else
    echo "m range: [$m_min, $m_max]"
  fi
  echo "n range: [$n_min, $n_max]"
  echo "max total m: $max_total_m"
  echo "i mode: $i_mode"
  echo "max cache size: $max_cache_size"
  if [[ -n "$resume_csv" ]]; then
    echo "resume csv: $resume_csv"
  fi
  echo "progress file: $progress_file"
  echo "stop on counterexample: $stop_on_counterexample"
  echo "classic-check mode: $classic_check"
  echo "yao prune enabled: $(( no_yao_prune == 0 ))"
  echo "disable optimistic yao prune: $no_yao_prune"
  echo
} >> "$log_file"

if (( do_build == 1 )); then
  echo "Building release binary..."
  cargo build --release
fi

if [[ ! -x "$bin_path" ]]; then
  echo "Binary not found or not executable: $bin_path" >&2
  echo "Try removing --no-build or passing --bin path/to/selection_generator" >&2
  exit 1
fi

total_runs=0
counterexamples=0
equal_count=0
implementation_bug_count=0
unknown_count=0
failed_count=0
skipped_completed=0
skipped_over_max_total=0

declare -A completed=()

load_completed_keys() {
  local source_file="$1"
  local m_col="$2"
  local n_col="$3"
  local i_col="$4"
  local yao_col="$5"
  local status_col="$6"
  if [[ ! -f "$source_file" ]]; then
    return
  fi
  while IFS=, read -r m n i; do
    completed["$m,$n,$i"]=1
  done < <(
    awk -F, -v m="$m_col" -v n="$n_col" -v i="$i_col" -v y="$yao_col" -v s="$status_col" '
      NR > 1 &&
      $m ~ /^[0-9]+$/ &&
      $n ~ /^[0-9]+$/ &&
      $i ~ /^[0-9]+$/ &&
      $y ~ /^[0-9]+$/ &&
      ($s == "equal" || $s == "counterexample" || $s == "implementation_bug") {
        print $m "," $n "," $i
      }
    ' "$source_file"
  )
}

update_progress_index() {
  local m="$1"
  local n="$2"
  local i="$3"
  local yao_result="$4"
  local classic_known="$5"
  local status="$6"
  local now="$7"
  local run_csv="$8"
  local tmp_file
  tmp_file="$(mktemp /tmp/yao_progress_index.XXXXXX)"
  awk -F, -v OFS=, -v m="$m" -v n="$n" -v i="$i" -v y="$yao_result" -v c="$classic_known" -v s="$status" -v ts="$now" -v csv="$run_csv" '
    NR == 1 { print; next }
    !($1 == m && $2 == n && $3 == i) { print }
    END { print m, n, i, y, c, s, ts, csv }
  ' "$progress_file" > "$tmp_file"
  {
    head -n 1 "$tmp_file"
    tail -n +2 "$tmp_file" | sort -t, -k2,2n -k1,1n -k3,3n
  } > "$progress_file"
  rm -f "$tmp_file"
}

write_progress_summary() {
  {
    echo "m,n,i,yao_result,classic_known,status,last_timestamp,last_csv"
    awk -F, 'NR > 1 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {print}' "$progress_file" \
      | sort -t, -k2,2n -k1,1n -k3,3n
  } > "$progress_summary_file"
}

seed_progress_from_results_csv() {
  local source_csv="$1"
  if [[ ! -f "$source_csv" ]]; then
    return
  fi
  while IFS=, read -r ts m n i y c s; do
    if [[ "$m" =~ ^[0-9]+$ ]] &&
       [[ "$n" =~ ^[0-9]+$ ]] &&
       [[ "$i" =~ ^[0-9]+$ ]] &&
       [[ "$y" =~ ^[0-9]+$ ]] &&
       [[ "$s" == "equal" || "$s" == "counterexample" || "$s" == "implementation_bug" ]]; then
      update_progress_index "$m" "$n" "$i" "$y" "$c" "$s" "$ts" "$source_csv"
    fi
  done < <(
    awk -F, 'NR > 1 { print $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $7 }' "$source_csv"
  )
}

seed_progress_from_results_csv "$csv_file"
load_completed_keys "$progress_file" 1 2 3 4 6
load_completed_keys "$csv_file" 2 3 4 5 7

tmp_runs_file="$(mktemp /tmp/yao_runs.XXXXXX)"

for plan in "${SWEEP_PLAN[@]}"; do
  IFS=, read -r plan_n plan_i_1based _ plan_m_max <<< "$plan"

  if (( plan_n < n_min || plan_n > n_max )); then
    continue
  fi

  plan_i=$((plan_i_1based - 1))
  if (( plan_i < 0 || plan_i >= plan_n )); then
    continue
  fi

  m_start="$plan_n"
  m_end="$plan_m_max"

  if [[ -n "$extra_min" ]]; then
    extra_start=$((plan_n + extra_min))
    extra_end=$((plan_n + extra_max))
    if (( m_start < extra_start )); then
      m_start="$extra_start"
    fi
    if (( m_end > extra_end )); then
      m_end="$extra_end"
    fi
  else
    if (( m_start < m_min )); then
      m_start="$m_min"
    fi
    if (( m_end > m_max )); then
      m_end="$m_max"
    fi
  fi

  if (( m_end > max_total_m )); then
    m_end="$max_total_m"
  fi

  if (( m_start > m_end )); then
    continue
  fi

  for ((m = m_start; m <= m_end; m++)); do
    if (( m > max_total_m )); then
      skipped_over_max_total=$((skipped_over_max_total + 1))
      continue
    fi

    key="${m},${plan_n},${plan_i}"
    if [[ -n "${completed[$key]+x}" ]]; then
      skipped_completed=$((skipped_completed + 1))
      continue
    fi

    printf "%d,%d,%d\n" "$m" "$plan_n" "$plan_i" >> "$tmp_runs_file"
  done
done

if [[ ! -s "$tmp_runs_file" ]]; then
  write_progress_summary
  echo "No pending runs (everything already completed or filtered)."
  echo "csv file: $csv_file"
  rm -f "$tmp_runs_file"
  exit 0
fi

mapfile -t run_specs < "$tmp_runs_file"
rm -f "$tmp_runs_file"

planned_runs="${#run_specs[@]}"

{
  echo "planned pending runs: $planned_runs"
  echo "skipped from existing csv: $skipped_completed"
  echo "skipped by max total m: $skipped_over_max_total"
  echo
} >> "$log_file"

for spec in "${run_specs[@]}"; do
  IFS=, read -r m n i <<< "$spec"
  total_runs=$((total_runs + 1))
  echo "[$total_runs/$planned_runs] running m=$m n=$n i=$i"

  cmd=(
    "$bin_path"
    --search-mode forward_yao
    -n "$m"
    --subset-n "$n"
    -i "$i"
    --single
    --max-cache-size "$max_cache_size"
  )
  if (( classic_check == 1 )); then
    cmd+=(--yao-classic-check)
  fi
  if (( no_yao_prune == 1 )); then
    cmd+=(--yao-disable-optimistic-prune)
  fi

  echo "cmd: ${cmd[*]}" >> "$log_file"

  tmp_run_output="$(mktemp /tmp/yao_run_output.XXXXXX)"
  set +e
  "${cmd[@]}" 2>&1 | tee "$tmp_run_output"
  exit_code=${PIPESTATUS[0]}
  set -e
  output="$(cat "$tmp_run_output")"
  rm -f "$tmp_run_output"

  {
    echo "----- BEGIN OUTPUT m=$m n=$n i=$i -----"
    echo "$output"
    echo "----- END OUTPUT m=$m n=$n i=$i -----"
  } >> "$log_file"

  now="$(date -Iseconds)"

  if (( exit_code != 0 )); then
    failed_count=$((failed_count + 1))
    echo "run failed (exit=$exit_code) for m=$m n=$n i=$i"
    printf "%s,%d,%d,%d,,,%s\n" "$now" "$m" "$n" "$i" "failed_exit_${exit_code}" >> "$csv_file"
    update_progress_index "$m" "$n" "$i" "" "" "failed_exit_${exit_code}" "$now" "$csv_file"
    continue
  fi

  # Exact Yao value is printed as "Comparisons: X" in full-search mode.
  # In --classic-check mode this line is typically absent, so we infer exact
  # values from decision output when possible (e.g. "No benefit here ... = classic = X").
  yao_result="$(printf '%s\n' "$output" | sed -n 's/^Comparisons: \([0-9][0-9]*\)$/\1/p' | tail -n 1)"
  classic_known="$(printf '%s\n' "$output" | sed -n 's/^Classic known value.*: \([0-9][0-9]*\)$/\1/p' | tail -n 1)"

  if printf '%s\n' "$output" | grep -q "Counterexample found"; then
    counterexamples=$((counterexamples + 1))
    status="counterexample"
    line="$(printf '%s\n' "$output" | grep "Counterexample found" | tail -n 1)"
    echo "ALERT: $line"
    echo "$line" >> "$counter_file"
    printf '\a'
    if command -v notify-send >/dev/null 2>&1; then
      notify-send "Yao counterexample found" "$line"
    fi
  elif printf '%s\n' "$output" | grep -q "No benefit here"; then
    equal_count=$((equal_count + 1))
    status="equal"
  elif printf '%s\n' "$output" | grep -q "Yao result is above classic"; then
    implementation_bug_count=$((implementation_bug_count + 1))
    status="implementation_bug"
    line="$(printf '%s\n' "$output" | grep "Yao result is above classic" | tail -n 1)"
    echo "BUG ALERT: $line"
    echo "$line" >> "$bug_file"
    printf '\a'
    if command -v notify-send >/dev/null 2>&1; then
      notify-send "Yao implementation bug candidate" "$line"
    fi
  else
    unknown_count=$((unknown_count + 1))
    status="unknown"
  fi

  # Fallback parsing for exact values when "Comparisons: X" is not present.
  # This covers classic-check "No benefit here: ... = classic = X" and
  # full-search messages that still contain an explicit "=" value.
  if [[ -z "$yao_result" ]]; then
    yao_result="$(printf '%s\n' "$output" | sed -n 's/^No benefit here: Yao(.*) = classic = \([0-9][0-9]*\)$/\1/p' | tail -n 1)"
  fi
  if [[ -z "$yao_result" ]]; then
    yao_result="$(printf '%s\n' "$output" | sed -n 's/^Counterexample found: Yao(.*) = \([0-9][0-9]*\) < classic .*/\1/p' | tail -n 1)"
  fi
  if [[ -z "$yao_result" ]]; then
    yao_result="$(printf '%s\n' "$output" | sed -n 's/^Yao result is above classic here: Yao(.*) = \([0-9][0-9]*\), classic = .*/\1/p' | tail -n 1)"
  fi

  printf "%s,%d,%d,%d,%s,%s,%s\n" \
    "$now" "$m" "$n" "$i" "${yao_result:-}" "${classic_known:-}" "$status" >> "$csv_file"
  update_progress_index "$m" "$n" "$i" "${yao_result:-}" "${classic_known:-}" "$status" "$now" "$csv_file"
  if [[ "${yao_result:-}" =~ ^[0-9]+$ ]] && [[ "$status" == "equal" || "$status" == "counterexample" || "$status" == "implementation_bug" ]]; then
    completed["$m,$n,$i"]=1
  fi

  if (( stop_on_counterexample == 1 )) && [[ "$status" == "counterexample" ]]; then
    write_progress_summary
    echo "Stopping on first counterexample as requested."
    echo "Log: $log_file"
    echo "CSV: $csv_file"
    echo "Counterexamples: $counter_file"
    exit 0
  fi

  if [[ "$status" == "implementation_bug" ]]; then
    write_progress_summary
    echo "Stopping due to implementation bug condition (Yao > classic)."
    echo "Log: $log_file"
    echo "CSV: $csv_file"
    echo "Implementation bug log: $bug_file"
    exit 2
  fi
done

write_progress_summary

echo
echo "Sweep complete."
echo "executed runs:      $total_runs"
echo "planned runs:       $planned_runs"
echo "skipped completed:  $skipped_completed"
echo "skipped m>$max_total_m: $skipped_over_max_total"
echo "counterexamples:   $counterexamples"
echo "equal:             $equal_count"
echo "implementation bug: $implementation_bug_count"
echo "unknown:           $unknown_count"
echo "failed:            $failed_count"
echo "log file:          $log_file"
echo "csv file:          $csv_file"
echo "progress file:     $progress_file"
echo "progress summary:  $progress_summary_file"
if (( counterexamples > 0 )); then
  echo "counterexample log: $counter_file"
fi
if (( implementation_bug_count > 0 )); then
  echo "implementation bug log: $bug_file"
fi
