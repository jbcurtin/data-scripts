import os
import subprocess
import time
import typing

def run_command(cmd: str, block: bool = False, expected_errors: typing.List[int] = []) -> str:
    if isinstance(cmd, str):
        cmd = [cmd]

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if block is False:
        return proc.pid

    while proc.poll() is None:
        time.sleep(.1)
        continue

    if proc.poll() > 0:
        if proc.poll() in expected_errors:
            return proc.stdout.read().decode('utf-8')

        raise NotImplementedError(f'Proc[{proc.poll()}]')

    return proc.stdout.read().decode('utf-8')

def write_script(script: str, path: str) -> None:
    dirpath: str = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    with open(path, 'w') as stream:
        stream.write(script)

def get_pid(program_name: str) -> int:
    if program_name == 'grep':
        raise NotImplementedError

    try:
        cmd = "ps aux|grep '%s'|grep -v grep|grep sh|awk '{print $2}'" % program_name
        cmd_result: int = int(run_command(cmd, True, [1]).strip('\n'))
        return cmd_result

    except ValueError:
        try:
            cmd = "ps aux|grep '%s'|grep -v grep|awk '{print $2}'" % program_name
            cmd_result = run_command(cmd, True, [1]).strip('\n')
            return int(cmd_result)

        except ValueError:
            return None

