#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run a fixed Yao sweep slice:
  - n = 12..14 with the first two table i-lines and extra = 1..2
  - n = 15 with the first two table i-lines and extra = 1
  - m = n is excluded

Usage:
  scripts/run_yao_slice_12_15.sh [options]

Options:
  --max-cache-size BYTES      Forward cache size (default: 4294967296 / 4 GiB)
  --resume-csv PATH           Resume from existing CSV and append new rows
  --progress-file PATH        Progress index to skip finished runs
                             (default: logs/yao/yao_progress_index.csv)
  --stop-on-counterexample    Stop immediately when first counterexample is found
  --classic-check             Enable --yao-classic-check
  --no-yao-prune              Enable --yao-disable-optimistic-prune
  --no-build                  Skip `cargo build --release`
  --bin PATH                  Binary path (default: ./target/release/selection_generator)
  -h, --help                  Show this help
EOF
}

max_cache_size=4294967296
resume_csv=""
log_root="logs/yao"
progress_file="${log_root}/yao_progress_index.csv"
stop_on_counterexample=0
classic_check=0
no_yao_prune=0
do_build=1
bin_path="./target/release/selection_generator"

# Fields: n, i_1based_from_table, max_extra
SLICE_LINES=(
  "12,2,2"
  "12,3,2"
  "13,2,2"
  "13,3,2"
  "14,2,2"
  "14,3,2"
  "15,2,1"
  "15,3,1"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
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
  log_file="${log_root}/yao_slice_12_15_resume_${run_id}.log"
else
  csv_file="${log_root}/yao_slice_12_15_${run_id}.csv"
  log_file="${log_root}/yao_slice_12_15_${run_id}.log"
  echo "timestamp,m,n,i,yao_result,classic_known,status" > "$csv_file"
fi
counter_file="${log_root}/yao_slice_12_15_counterexamples_${run_id}.txt"
bug_file="${log_root}/yao_slice_12_15_implementation_bugs_${run_id}.txt"
progress_summary_file="${progress_file%.csv}.summary.txt"

if [[ ! -s "$csv_file" ]]; then
  echo "timestamp,m,n,i,yao_result,classic_known,status" > "$csv_file"
fi

if [[ ! -f "$progress_file" ]]; then
  echo "m,n,i,yao_result,classic_known,status,last_timestamp,last_csv" > "$progress_file"
fi

if command -v flock >/dev/null 2>&1; then
  lock_file="${csv_file}.lock"
  exec 9>"$lock_file"
  if ! flock -n 9; then
    echo "Another sweep process is already writing to $csv_file (lock: $lock_file)" >&2
    exit 1
  fi
fi

if command -v flock >/dev/null 2>&1; then
  progress_lock_file="${progress_file}.lock"
  exec 8>"$progress_lock_file"
  flock 8
fi

{
  echo "Yao slice 12..15 started at $(date -Iseconds)"
  echo "slice lines:"
  printf '  %s\n' "${SLICE_LINES[@]}"
  echo "max cache size: $max_cache_size"
  if [[ -n "$resume_csv" ]]; then
    echo "resume csv: $resume_csv"
  fi
  echo "progress file: $progress_file"
  echo "stop on counterexample: $stop_on_counterexample"
  echo "classic-check mode: $classic_check"
  echo "yao prune enabled: $(( no_yao_prune == 0 ))"
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

tmp_runs_file="$(mktemp /tmp/yao_slice_12_15.XXXXXX)"

for line in "${SLICE_LINES[@]}"; do
  IFS=, read -r n i_1based max_extra <<< "$line"
  i=$((i_1based - 1))

  for ((extra = 1; extra <= max_extra; extra++)); do
    m=$((n + extra))
    if (( m > 16 )); then
      continue
    fi

    key="${m},${n},${i}"
    if [[ -n "${completed[$key]+x}" ]]; then
      skipped_completed=$((skipped_completed + 1))
      continue
    fi

    printf "%d,%d,%d\n" "$m" "$n" "$i" >> "$tmp_runs_file"
  done
done

if [[ ! -s "$tmp_runs_file" ]]; then
  write_progress_summary
  echo "No pending runs (everything already completed)."
  echo "csv file: $csv_file"
  rm -f "$tmp_runs_file"
  exit 0
fi

mapfile -t run_specs < "$tmp_runs_file"
rm -f "$tmp_runs_file"

planned_runs="${#run_specs[@]}"

{
  echo "planned pending runs: $planned_runs"
  echo "skipped completed: $skipped_completed"
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
echo "Slice complete."
echo "executed runs:      $total_runs"
echo "planned runs:       $planned_runs"
echo "skipped completed:  $skipped_completed"
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
