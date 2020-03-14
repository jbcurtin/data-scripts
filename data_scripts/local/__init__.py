import os
import signal
import subprocess
import time

from data_scripts.local import utils as local_utils

DASK_WORKING_PATH: str = '/tmp/dask-scheduler'
INSTALL_DASK: str = """if [ ! -d env ]; then
    virtualenv -p $(which python3) env
    source env/bin/activate
    pip install dask[distributed] numpy
fi
source env/bin/activate
"""

def get_dask_worker_pid() -> int:
    return local_utils.get_pid('env/bin/dask-worker')

def get_dask_scheduler_pid() -> int:
    return local_utils.get_pid('env/bin/dask-scheduler')

def setup_dask_scheduler() -> int:
    script_path = f'{DASK_WORKING_PATH}/run-schedular.sh'
    script = f"""#!/usr/bin/env bash
cd {DASK_WORKING_PATH}
{INSTALL_DASK}
dask-scheduler
exit 0
"""
    pid = get_dask_scheduler_pid()
    if pid is None:
        local_utils.write_script(script, script_path)
        pid = local_utils.run_command(f'bash {script_path}')

    return pid

def setup_dask_worker() -> int:
    script_path = f'{DASK_WORKING_PATH}/run-worker.sh'
    script = f"""#!/usr/bin/env bash
cd {DASK_WORKING_PATH}
{INSTALL_DASK}
dask-worker 127.0.0.1:8786
exit 0
"""
    pid = get_dask_worker_pid()
    if pid is None:
        local_utils.write_script(script, script_path)
        pid = local_utils.run_command(f'bash {script_path}')

    return pid

def setup_dask():
    scheduler_pid = setup_dask_scheduler()
    time.sleep(.3)
    worker_pid = setup_dask_worker()
    return f'Sch: {scheduler_pid} - Work: {worker_pid}'

def destroy_dask():
    worker_pid = get_dask_worker_pid()
    if worker_pid:
        os.kill(worker_pid, signal.SIGKILL)

    scheduler_pid = get_dask_scheduler_pid()
    if scheduler_pid:
        os.kill(scheduler_pid, signal.SIGKILL)

