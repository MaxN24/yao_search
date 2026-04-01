#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Compare Yao `level4` pruning against `no-prune` on a grid of instances.

Usage:
  scripts/run_yao_prune_compare.sh [options]

Options:
  --m-min INT                 Minimum total poset size m (default: 3)
  --m-max INT                 Maximum total poset size m (default: 16)
  --n-min INT                 Minimum subset size n (default: 3)
  --n-max INT                 Maximum subset size n (default: 16)
  --max-total-m INT           Hard cap for total poset size m (default: 16)
  --extra-min INT             Minimum extra elements (m-n). If set, uses m=n+extra.
  --extra-max INT             Maximum extra elements (m-n). Must be used with --extra-min.
  --i-mode all|half           all: i=0..n-1, half: i=0..floor((n-1)/2) (default: half)
  --max-cache-size BYTES      Forward cache size (default: 4294967296 / 4 GiB)
  --resume-csv PATH           Resume from existing comparison CSV and skip completed (m,n,i)
  --no-build                  Skip `cargo build --release`
  --bin PATH                  Binary path (default: ./target/release/selection_generator)
  --keep-going-on-mismatch    Do not stop when level4 and no-prune disagree
  -h, --help                  Show this help

Outputs:
  CSV columns:
    timestamp,m,n,i,level4_comparisons,level4_ms,no_prune_comparisons,no_prune_ms,delta_ms,status
  Summary file:
    same basename as CSV with `.summary.txt`
EOF
}

m_min=3
m_max=16
n_min=3
n_max=16
max_total_m=16
extra_min=""
extra_max=""
i_mode="half"
max_cache_size=4294967296
resume_csv=""
do_build=1
bin_path="./target/release/selection_generator"
keep_going_on_mismatch=0
log_root="logs/yao"

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
    --no-build)
      do_build=0
      shift
      ;;
    --bin)
      bin_path="$2"
      shift 2
      ;;
    --keep-going-on-mismatch)
      keep_going_on_mismatch=1
      shift
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

if (( m_min < 1 || m_max < 1 || n_min < 1 || n_max < 1 || max_total_m < 1 )); then
  echo "m/n bounds and --max-total-m must be >= 1" >&2
  exit 1
fi

if (( m_min > m_max || n_min > n_max )); then
  echo "Invalid bounds: min cannot be greater than max" >&2
  exit 1
fi

if [[ -n "$extra_min" || -n "$extra_max" ]]; then
  if [[ -z "$extra_min" || -z "$extra_max" ]]; then
    echo "Both --extra-min and --extra-max must be set together" >&2
    exit 1
  fi
  if (( extra_min < 0 || extra_max < 0 || extra_min > extra_max )); then
    echo "Invalid extra range" >&2
    exit 1
  fi
fi

mkdir -p "$log_root"
timestamp="$(date +%Y%m%d_%H%M%S)"
run_id="${timestamp}_$$"

if [[ -n "$resume_csv" ]]; then
  csv_file="$resume_csv"
  if [[ ! -f "$csv_file" ]]; then
    echo "--resume-csv file does not exist: $csv_file" >&2
    exit 1
  fi
  log_file="${log_root}/yao_prune_compare_resume_${run_id}.log"
else
  csv_file="${log_root}/yao_prune_compare_${run_id}.csv"
  log_file="${log_root}/yao_prune_compare_${run_id}.log"
  echo "timestamp,m,n,i,level4_comparisons,level4_ms,no_prune_comparisons,no_prune_ms,delta_ms,status" > "$csv_file"
fi

summary_file="${csv_file%.csv}.summary.txt"

if command -v flock >/dev/null 2>&1; then
  lock_file="${csv_file}.lock"
  exec 9>"$lock_file"
  if ! flock -n 9; then
    echo "Another compare process is already writing to $csv_file (lock: $lock_file)" >&2
    exit 1
  fi
fi

declare -A done_keys=()
checked=0
matches=0
mismatches=0
sum_level4_ms=0
sum_no_prune_ms=0
last_case=""
last_status=""

if [[ -f "$csv_file" ]]; then
  while IFS=, read -r ts m n i level4_cmp level4_ms no_prune_cmp no_prune_ms delta_ms status; do
    if [[ "$ts" == "timestamp" ]]; then
      continue
    fi
    key="${m},${n},${i}"
    done_keys["$key"]=1
    checked=$((checked + 1))
    sum_level4_ms=$((sum_level4_ms + level4_ms))
    sum_no_prune_ms=$((sum_no_prune_ms + no_prune_ms))
    if [[ "$status" == "match" ]]; then
      matches=$((matches + 1))
    else
      mismatches=$((mismatches + 1))
    fi
    last_case="m=${m} n=${n} i=${i}"
    last_status="$status"
  done < "$csv_file"
fi

update_summary() {
  local avg_level4="0.00"
  local avg_no_prune="0.00"
  if (( checked > 0 )); then
    avg_level4=$(awk -v s="$sum_level4_ms" -v c="$checked" 'BEGIN { printf "%.2f", s / c }')
    avg_no_prune=$(awk -v s="$sum_no_prune_ms" -v c="$checked" 'BEGIN { printf "%.2f", s / c }')
  fi

  cat > "$summary_file" <<EOF
updated_at=$(date -Iseconds)
csv_file=$csv_file
checked=$checked
matches=$matches
mismatches=$mismatches
avg_level4_ms=$avg_level4
avg_no_prune_ms=$avg_no_prune
last_case=$last_case
last_status=$last_status
EOF
}

run_mode() {
  local mode_name="$1"
  shift

  local tmp
  tmp="$(mktemp /tmp/yao_prune_compare.XXXXXX)"
  local start_ns end_ns elapsed_ms comparisons

  start_ns="$(date +%s%N)"
  if ! "$@" >"$tmp" 2>&1; then
    cat "$tmp" >&2
    rm -f "$tmp"
    return 1
  fi
  end_ns="$(date +%s%N)"
  elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
  comparisons="$(awk '/^Comparisons:/ {print $2; exit}' "$tmp")"

  {
    echo "===== $(date -Iseconds) mode=$mode_name ====="
    cat "$tmp"
    echo
  } >> "$log_file"

  rm -f "$tmp"

  if [[ -z "$comparisons" ]]; then
    echo "Could not parse Comparisons: for mode=$mode_name" >&2
    return 1
  fi

  printf '%s %s\n' "$comparisons" "$elapsed_ms"
}

if (( do_build )); then
  cargo build --release >> "$log_file" 2>&1
fi

declare -a run_specs=()
for n in $(seq "$n_min" "$n_max"); do
  if (( n > max_total_m )); then
    continue
  fi

  if [[ -n "$extra_min" ]]; then
    for extra in $(seq "$extra_min" "$extra_max"); do
      m=$((n + extra))
      if (( m < m_min || m > m_max || m > max_total_m )); then
        continue
      fi

      if [[ "$i_mode" == "half" ]]; then
        i_max=$(((n - 1) / 2))
      else
        i_max=$((n - 1))
      fi

      for i in $(seq 0 "$i_max"); do
        run_specs+=("${m},${n},${i}")
      done
    done
  else
    start_m=$m_min
    if (( start_m < n )); then
      start_m=$n
    fi
    end_m=$m_max
    if (( end_m > max_total_m )); then
      end_m=$max_total_m
    fi

    for m in $(seq "$start_m" "$end_m"); do
      if [[ "$i_mode" == "half" ]]; then
        i_max=$(((n - 1) / 2))
      else
        i_max=$((n - 1))
      fi

      for i in $(seq 0 "$i_max"); do
        run_specs+=("${m},${n},${i}")
      done
    done
  fi
done

total_specs="${#run_specs[@]}"
pending_specs=0
for spec in "${run_specs[@]}"; do
  if [[ -z "${done_keys[$spec]+x}" ]]; then
    pending_specs=$((pending_specs + 1))
  fi
done

echo "Comparison run started at $(date -Iseconds)" | tee -a "$log_file"
echo "CSV: $csv_file" | tee -a "$log_file"
echo "Summary: $summary_file" | tee -a "$log_file"
echo "Pending cases: $pending_specs / $total_specs" | tee -a "$log_file"

update_summary

case_idx=0
for spec in "${run_specs[@]}"; do
  if [[ -n "${done_keys[$spec]+x}" ]]; then
    continue
  fi

  IFS=, read -r m n i <<< "$spec"
  case_idx=$((case_idx + 1))
  echo "[$case_idx/$pending_specs] m=$m n=$n i=$i" | tee -a "$log_file"

  read -r level4_cmp level4_ms < <(
    run_mode \
      level4 \
      "$bin_path" \
      --search-mode forward_yao \
      -n "$m" \
      --subset-n "$n" \
      -i "$i" \
      --single \
      --max-cache-size "$max_cache_size"
  )

  read -r no_prune_cmp no_prune_ms < <(
    run_mode \
      no-prune \
      "$bin_path" \
      --search-mode forward_yao \
      -n "$m" \
      --subset-n "$n" \
      -i "$i" \
      --single \
      --max-cache-size "$max_cache_size" \
      --yao-disable-optimistic-prune
  )

  delta_ms=$((no_prune_ms - level4_ms))
  status="match"
  if [[ "$level4_cmp" != "$no_prune_cmp" ]]; then
    status="mismatch"
  fi

  now_iso="$(date -Iseconds)"
  echo "${now_iso},${m},${n},${i},${level4_cmp},${level4_ms},${no_prune_cmp},${no_prune_ms},${delta_ms},${status}" >> "$csv_file"

  checked=$((checked + 1))
  sum_level4_ms=$((sum_level4_ms + level4_ms))
  sum_no_prune_ms=$((sum_no_prune_ms + no_prune_ms))
  last_case="m=${m} n=${n} i=${i}"
  last_status="$status"
  if [[ "$status" == "match" ]]; then
    matches=$((matches + 1))
  else
    mismatches=$((mismatches + 1))
  fi

  update_summary

  echo "  level4:   comparisons=$level4_cmp time_ms=$level4_ms" | tee -a "$log_file"
  echo "  no-prune: comparisons=$no_prune_cmp time_ms=$no_prune_ms" | tee -a "$log_file"
  echo "  delta_ms(no_prune-level4)=$delta_ms status=$status" | tee -a "$log_file"

  if [[ "$status" == "mismatch" && "$keep_going_on_mismatch" -eq 0 ]]; then
    echo "Stopping on mismatch at m=$m n=$n i=$i" | tee -a "$log_file"
    exit 1
  fi
done

echo "Comparison run finished at $(date -Iseconds)" | tee -a "$log_file"
update_summary
