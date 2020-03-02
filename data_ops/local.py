import os
import signal
import subprocess
import time

def run_command(cmd: str, block: bool = False) -> None:
    proc = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if block is False:
        return proc.pid

    while proc.poll() is None:
        time.sleep(.1)
        continue

    if proc.poll() > 0:
        raise NotImplementedError(f'Proc[{proc.poll()}]')

    return proc.stdout.read().decode('utf-8')

def write_script(script: str, path: str) -> None:
    dirpath: str = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    with open(path, 'w') as stream:
        stream.write(script)

DASK_WORKING_PATH: str = '/tmp/dask-scheduler'

def _get_dask_pid() -> int:
    try:
        return int(run_command("ps aux|grep dask-scheduler|grep -v grep|grep -v run|awk '{print $2}'", True).strip())

    except ValueError:
        return None

def check_local_dask_schedular() -> bool:
    return not _get_dask_pid() is None

def destroy_local_dask_scheduler():
    pid = _get_dask_pid()
    if pid:
        os.kill(pid, signal.SIGKILL)

def setup_local_dask_scheduler():
    script_path = f'{DASK_WORKING_PATH}/run.sh'
    script = f"""#!/usr/bin/env bash
cd {DASK_WORKING_PATH}
if [ ! -d env ]; then
    virtualenv -p $(which python3) env
    source env/bin/activate
    pip install dask[distributed]
fi
source env/bin/activate
dask-scheduler
exit 0
"""
    pid = _get_dask_pid()
    if _get_dask_pid() is None:
        write_script(script, script_path)
        pid = run_command(f'bash {script_path}')
        # os.remove(script_path)
        return f'dask-scheduler started: {pid}'

    return f'dask-scheduler running: {pid}'

