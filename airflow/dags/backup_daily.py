"""01:20 UTC: 어제 컬럼형 Parquet(export_columnar 산출, parquet/raw_*/dt=)을 Oracle /mnt/backup/mp2/parquet 로 rsync, dry-run 0 으로 검증, 원격 보존 N일.
v1 교훈(docs/00 §5 10-07·10-29): pipefail, 검증은 '전송했다'가 아니라 '원격이 로컬과 같다', 공간은 보내기 전에 확인."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from mp2_common import DEFAULT_ARGS

REMOTE = "ubuntu@10.88.0.1"
RROOT = "/mnt/backup/mp2/parquet"
SSH = "ssh -i /opt/airflow/secrets/oci_key -o UserKnownHostsFile=/opt/airflow/secrets/known_hosts -o StrictHostKeyChecking=yes -o ConnectTimeout=15"
KEEP_DAYS_REMOTE = 14   # 측정 뒤 조정. 11-24: Oracle 여유 32GB

with DAG("backup_daily", default_args=DEFAULT_ARGS, schedule="20 1 * * *", start_date=datetime(2026, 11, 24), catchup=False,
         max_active_runs=1, tags=["storage", "backup"], params={"target_date": ""}) as dag:
    day = "{{ params.target_date or ds }}"
    check_space = BashOperator(task_id="check_remote_space", bash_command=f'''set -euo pipefail
need=$(du -sm /data/parquet/raw_*/dt={day} 2>/dev/null | awk '{{s+=$1}} END{{print s+0}}')
free=$({SSH} {REMOTE} "df -m /mnt/backup | awk 'NR==2{{print \\$4}}'")
echo "need=${{need}}MB free=${{free}}MB"
[ "$need" -gt 0 ] || {{ echo "nothing to back up for {day}"; exit 0; }}
[ "$free" -gt $((need * 2)) ] || {{ echo "not enough remote space"; exit 1; }}''')
    sync = BashOperator(task_id="sync_to_oracle", bash_command=f'''set -euo pipefail
for d in /data/parquet/raw_*/dt={day}; do
  [ -d "$d" ] || continue
  rel=${{d#/data/parquet/}}
  {SSH} {REMOTE} "mkdir -p {RROOT}/$rel"
  rsync -a --partial -e "{SSH}" "$d/" {REMOTE}:{RROOT}/$rel/
done''')
    verify = BashOperator(task_id="verify_remote_in_sync", bash_command=f'''set -euo pipefail
n=0
for d in /data/parquet/raw_*/dt={day}; do
  [ -d "$d" ] || continue
  rel=${{d#/data/parquet/}}
  c=$(rsync -rltn --itemize-changes -e "{SSH}" "$d/" {REMOTE}:{RROOT}/$rel/ | grep -cE '^[<>ch*]' || true)
  n=$((n + c))
done
echo "files differing: $n"; [ "$n" -eq 0 ]''')
    retention = BashOperator(task_id="apply_remote_retention", bash_command=f'''set -euo pipefail
{SSH} {REMOTE} 'cutoff=$(date -u -d "-{KEEP_DAYS_REMOTE} days" +%F); for d in $(ls -d {RROOT}/*/*/dt=* 2>/dev/null); do day=${{d##*dt=}}; [[ "$day" < "$cutoff" ]] && {{ echo "remove $d"; rm -rf "$d"; }}; done; df -h /mnt/backup | tail -1' ''')
    check_space >> sync >> verify >> retention
