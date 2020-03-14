import pytest

def test_run_command_block():
    import os
    import tempfile

    from data_scripts.local import utils as local_utils

    filename: str = tempfile.NamedTemporaryFile().name
    if os.path.exists(filename):
        raise IOError(f'File[{filename}] should not exist')

    output: str = local_utils.run_command(f'touch {filename}', True)

    if not os.path.exists(filename):
        raise IOError(f'File[{filename}] should exist')

def test_run_command_non_block():
    import os
    import tempfile
    import time

    from data_scripts.local import utils as local_utils
    filename: str = tempfile.NamedTemporaryFile().name

    if os.path.exists(filename):
        raise IOError(f'File[{filename}] should not exist')

    cmd: str = f'dd if=/dev/zero of={filename} bs=1MB count=512'
    pid: int = local_utils.run_command(cmd, False)
    while True:
        cmd_check: str = f'ps aux|grep {filename}|grep -v grep'
        result_check:str = local_utils.run_command(cmd_check, True, [1])
        if result_check == '':
            break

        pid_check: int = int(result_check.split(' ')[1])
        assert pid_check == pid
        time.sleep(.1)

    os.remove(filename)

def test_write_script():
    import os
    import tempfile

    from data_scripts.local import utils as local_utils

    filename: str = tempfile.NamedTemporaryFile().name
    if os.path.exists(filename):
        raise IOError(f'File[{filename}] should not exist')

    script = 'awesome'
    local_utils.write_script(script, filename)
    with open(filename, 'r') as stream:
        assert stream.read() == script

    os.remove(filename)

def test_get_pid():
    import os
    import tempfile
    import time

    from data_scripts.local import utils as local_utils

    filename: str = tempfile.NamedTemporaryFile().name
    cmd: str = f'dd if=/dev/zero of={filename} bs=1MB count=512'
    pid: int = local_utils.run_command(cmd, False)
    found: bool = False
    while True:
        cmd_check = f'ps aux|grep {filename}|grep -v grep'
        result_check = local_utils.run_command(cmd_check, True, [1])
        pid_check = local_utils.get_pid(filename)
        if pid_check == pid and str(pid_check) in result_check:
            time.sleep(.1)
            found = True
            continue

        assert pid_check is None
        break

    assert found is True
    os.remove(filename)

def test_get_dask_worker_pid():
    from data_scripts import local
    from data_scripts.local import utils as local_utils

    cmd: str = "ps aux|grep dask-worker|grep -v grep |awk '{print $2}'"
    try:
        existing_worker_pid: int = int(local_utils.run_command(cmd, True, [1]))
    except ValueError:
        existing_worker_pid = None
        local.setup_dask()

    worker_pid: int = local.get_dask_worker_pid()
    worker_pid_check: int = int(local_utils.run_command(cmd, True, [1]))
    if existing_worker_pid is None:
        local.destroy_dask()
        assert local.get_dask_worker_pid() is None

    assert worker_pid == worker_pid_check

def test_get_dask_scheduler_pid():
    from data_scripts import local
    from data_scripts.local import utils as local_utils

    cmd: str = "ps aux|grep 'env/bin/dask-scheduler'|grep -v grep|awk '{print $2}'"
    try:
        existing_scheduler_pid: int = int(local_utils.run_command(cmd, True, [1]).strip('\n'))
    except ValueError:
        existing_scheduler_pid = None
        local.setup_dask()

    scheduler_pid: int = local.get_dask_scheduler_pid()
    scheduler_pid_check: int = int(local_utils.run_command(cmd, True, [1]).strip('\n'))
    if existing_scheduler_pid is None:
        local.destroy_dask()
        assert local.get_dask_scheduler_pid() is None

    assert scheduler_pid == existing_scheduler_pid

