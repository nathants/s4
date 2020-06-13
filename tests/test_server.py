import os
import contextlib
import pool.proc
import pytest
import requests
import shell
import s4.cli
import s4.server
import time
import util.log
import util.time
from shell import run
from util.retry import retry

def rm_whitespace(x):
    return '\n'.join([y.strip()
                      for y in x.splitlines()
                      if y.strip()])

def start(port):
    with shell.cd(f'_{port}'):
        for i in range(5):
            try:
                s4.http_port = lambda: port + i
                s4.server.start()
                return
            except:
                continue
        assert False, f'failed to start server on ports from: {port}'

@contextlib.contextmanager
def servers():
    util.log.setup(format='%(message)s')
    shell.set['stream'] = True
    with util.time.timeout(10):
        with shell.tempdir():
            @retry
            def start_all():
                ports = [util.net.free_port() for _ in range(3)]
                s4.servers = lambda: [('0.0.0.0', str(port)) for port in ports]
                s4.conf_path = os.environ['S4_CONF_PATH'] = os.path.abspath(run('mktemp -p .'))
                with open(s4.conf_path, 'w') as f:
                    f.write('\n'.join(f'0.0.0.0:{port}' for port in ports) + '\n')
                procs = [pool.proc.new(start, port) for port in ports]
                try:
                    for _ in range(50):
                        try:
                            for port in ports:
                                requests.get(f'http://0.0.0.0:{port}')
                            break
                        except:
                            time.sleep(.1)
                    else:
                        assert False, 'failed to start servers'
                except:
                    for proc in procs:
                        proc.terminate()
                    raise
                else:
                    return procs
            procs = start_all()
            watch = True
            def watcher():
                while True:
                    for proc in procs:
                        if not proc.is_alive():
                            if not watch:
                                return
                            time.sleep(1)
                            print('proc died! exiting...', flush=True)
                            os._exit(1)
            pool.thread.new(watcher)
            try:
                yield
            finally:
                watch = False
                for proc in procs:
                    proc.terminate()

def test_spaces_are_not_allowed():
    with servers():
        with pytest.raises(SystemExit):
            run('echo | s4 cp - "s4://bucket/basic/dir/fi le.txt"')

def test_updates_are_not_allowed():
    with servers():
        path = 's4://bucket/basic/dir/file.txt'
        run('echo | s4 cp -', path)
        with pytest.raises(SystemExit):
            run('echo | s4 cp -', path)
        run('s4 rm', path)
        run('echo | s4 cp -', path)

def test_basic():
    with servers():
        run('echo 123 > file.txt')
        run('s4 cp file.txt s4://bucket/basic/dir/file.txt')
        run('echo 345 > file2.txt')
        run('s4 cp file2.txt s4://bucket/basic/dir/')
        assert run('s4 ls -r s4://bucket/ | cut -d" " -f4').splitlines() == [
            'basic/dir/file.txt',
            'basic/dir/file2.txt',
        ]
        run('s4 cp s4://bucket/basic/dir/file.txt out.txt')
        assert run('cat out.txt') == "123"
        run('s4 cp s4://bucket/basic/dir/file2.txt', 'out2.txt')
        assert run('cat out2.txt') == "345"
        run('mkdir foo/')
        run('s4 cp s4://bucket/basic/dir/file.txt foo/')
        assert run('cat foo/file.txt') == "123"

def test_cp_file_to_dot():
    with servers():
        run('echo foo > file.txt')
        run('s4 cp file.txt s4://bucket/file2.txt')
        run('s4 cp s4://bucket/file2.txt .')
        assert 'foo' == run('cat file2.txt')

def test_cp_dir_to_dot():
    with servers():
        run('echo | s4 cp - s4://bucket/dir1/file1.txt')
        run('echo | s4 cp - s4://bucket/dir2/file2.txt')
        run('echo | s4 cp - s4://bucket/dir2/file3.txt')
        assert run('s4 ls -r s4://bucket | cut -d" " -f4').splitlines() == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('s4 cp -r s4://bucket .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        run('s4 cp -r s4://bucket/dir2 .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        run('s4 cp -r s4://bucket/dir2/ .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]

def test_cp_dot_to_dot():
    with servers():
        with shell.tempdir():
            run('mkdir dir1 dir2')
            run('touch dir1/file1.txt dir2/file2.txt dir2/file3.txt')
            run('s4 cp -r . s4://bucket')
        assert run('s4 ls -r s4://bucket | cut -d" " -f4').splitlines() == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('s4 cp -r s4://bucket .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        run('s4 cp -r s4://bucket/dir2 .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        run('s4 cp -r s4://bucket/dir2/ .')
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]

def test_cp():
    with servers():
        run('mkdir -p foo/3')
        run('echo 123 > foo/1.txt')
        run('echo 234 > foo/2.txt')
        run('echo 456 > foo/3/4.txt')
        run('s4 cp -r foo/ s4://bucket/cp/dst/')
        assert rm_whitespace(run("s4 ls s4://bucket/cp/dst/ | awk '{print $NF}'")) == rm_whitespace("""
            1.txt
            2.txt
            3/
        """)
        assert run('s4 ls -r s4://bucket/cp/dst/ | cut -d" " -f4') == rm_whitespace("""
            cp/dst/1.txt
            cp/dst/2.txt
            cp/dst/3/4.txt
        """)
        run('s4 cp -r s4://bucket/cp/dst/ dst1/')
        assert run('grep ".*" $(find dst1/ -type f | sort)') == rm_whitespace("""
            dst1/1.txt:123
            dst1/2.txt:234
            dst1/3/4.txt:456
        """)
        run('s4 cp -r s4://bucket/cp/dst/ .')
        assert run('grep ".*" $(find dst/ -type f | sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)
        run('rm -rf dst')
        run('s4 cp -r foo s4://bucket/cp/dst2')
        assert rm_whitespace(run("s4 ls s4://bucket/cp/dst2/ | awk '{print $NF}'")) == rm_whitespace("""
            1.txt
            2.txt
            3/
        """)
        assert rm_whitespace(run('s4 ls -r s4://bucket/cp/dst2/ | cut -d" " -f4')) == rm_whitespace("""
            cp/dst2/1.txt
            cp/dst2/2.txt
            cp/dst2/3/4.txt
        """)
        run('s4 cp -r s4://bucket/cp/dst .')
        assert run('grep ".*" $(find dst/ -type f | sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)

def test_ls():
    with servers():
        run('echo | s4 cp - s4://bucket/other-listing/key0.txt')
        run('echo | s4 cp - s4://bucket/listing/dir1/key1.txt')
        run('echo | s4 cp - s4://bucket/listing/dir1/dir2/key2.txt')
        assert run('s4 ls s4://bucket/listing/dir1/ke | cut -d" " -f4') == rm_whitespace("""
            key1.txt
        """)
        assert rm_whitespace(run("s4 ls s4://bucket/listing/dir1/ | awk '{print $NF}'")) == rm_whitespace("""
            dir2/
            key1.txt
        """)
        assert rm_whitespace(run('s4 ls s4://bucket/listing/d')) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace(run('s4 ls s4://bucket/listing/')) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace(run('s4 ls -r s4://bucket/listing | cut -d" " -f4')) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        assert rm_whitespace(run('s4 ls -r s4://bucket/listing/ | cut -d" " -f4')) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        assert rm_whitespace(run('s4 ls -r s4://bucket/listing/d | cut -d" " -f4')) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        with pytest.raises(SystemExit):
            run('s4 ls s4://bucket/fake/')

def test_rm():
    with servers():
        run('s4 rm -r s4://bucket/rm/di')
        run('echo | s4 cp - s4://bucket/rm/dir1/key1.txt')
        run('echo | s4 cp - s4://bucket/rm/dir1/dir2/key2.txt')
        assert rm_whitespace(run('s4 ls -r s4://bucket/rm/ | cut -d" " -f4')) == rm_whitespace("""
            rm/dir1/dir2/key2.txt
            rm/dir1/key1.txt
        """)
        run('s4 rm s4://bucket/rm/dir1/key1.txt')
        assert rm_whitespace(run('s4 ls -r s4://bucket/rm/ | cut -d" " -f4')) == rm_whitespace("""
            rm/dir1/dir2/key2.txt
        """)
        run('s4 rm -r s4://bucket/rm/di')
        with pytest.raises(SystemExit):
            run('s4 ls -r s4://bucket/rm/')

def test_stdin():
    with servers():
        run('echo foo | s4 cp - s4://bucket/stdin/bar')
        assert 'foo' == run('s4 cp s4://bucket/stdin/bar -')

def test_binary():
    with servers():
        run('head -c10 /dev/urandom > input')
        run('s4 cp input s4://bucket/blob')
        run('s4 cp s4://bucket/blob output')
        a = run('cat input | xxh3', warn=True)['stdout']
        b = run('cat output | xxh3', warn=True)['stdout']
        assert a == b
        run('cat input | s4 cp - s4://bucket/blob2')
        run('s4 cp s4://bucket/blob2 - > output2')
        a = run('cat input | xxh3', warn=True)['stdout']
        b = run('cat output2 | xxh3', warn=True)['stdout']
        assert a == b
