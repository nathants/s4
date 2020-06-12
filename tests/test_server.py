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
                s4.conf_path = os.environ['S4_CONF_PATH'] = run('mktemp -p .')
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

def test_basic():
    with servers():
        with open('file.txt', 'w') as f:
            f.write('123')
        s4.cli.cp('file.txt', 's4://bucket/basic/dir/file.txt')
        with open('file2.txt', 'w') as f:
            f.write('345\n')
        s4.cli.cp('file2.txt', 's4://bucket/basic/dir/')
        assert s4.cli.ls('s4://bucket/', recursive=True) == [
            '_ _ _ basic/dir/file.txt',
            '_ _ _ basic/dir/file2.txt',
        ]
        s4.cli.cp('s4://bucket/basic/dir/file.txt', 'out.txt')
        run('cat out.txt') == "123"
        s4.cli.cp('s4://bucket/basic/dir/file2.txt', 'out2.txt')
        run('cat out.txt') == "345\n"
        run('mkdir foo/')
        s4.cli.cp('s4://bucket/basic/dir/file.txt', 'foo/')
        with open('foo/file.txt') as f:
            assert f.read() == "123"

def test_cp_file_to_dot():
    with servers():
        with open('file.txt', 'w') as f:
            f.write('foo')
        s4.cli.cp('file.txt', 's4://bucket/file.txt')
        s4.cli.cp('s4://bucket/file.txt', '.')
        assert 'foo' == run('cat file.txt')

def test_cp_dir_to_dot():
    with servers():
        s4.cli.cp('/dev/null', 's4://bucket/dir1/file1.txt')
        s4.cli.cp('/dev/null', 's4://bucket/dir2/file2.txt')
        s4.cli.cp('/dev/null', 's4://bucket/dir2/file3.txt')
        s4.cli.cp('s4://bucket', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        s4.cli.cp('s4://bucket/dir2', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        s4.cli.cp('s4://bucket/dir2/', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]

def test_cp_dot_to_dot():
    with servers():
        with shell.tempdir():
            run('mkdir dir1 dir2')
            run('touch dir1/file1.txt dir2/file2.txt dir2/file3.txt')
            s4.cli.cp('.', 's4://bucket', recursive=True)
        s4.cli.cp('s4://bucket', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir1/file1.txt',
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        s4.cli.cp('s4://bucket/dir2', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]
        run('rm -rf dir*')
        s4.cli.cp('s4://bucket/dir2/', '.', recursive=True)
        assert sorted(run('find dir* -type f').splitlines()) == [
            'dir2/file2.txt',
            'dir2/file3.txt',
        ]

def test_cp():
    with servers():
        run('mkdir -p foo/3')
        with open('foo/1.txt', 'w') as f:
            f.write('123')
        with open('foo/2.txt', 'w') as f:
            f.write('234')
        with open('foo/3/4.txt', 'w') as f:
            f.write('456')
        s4.cli.cp('foo/', 's4://bucket/cp/dst/', recursive=True)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/cp/dst/'))) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/cp/dst/', recursive=True))) == rm_whitespace("""
            _ _ _ cp/dst/1.txt
            _ _ _ cp/dst/2.txt
            _ _ _ cp/dst/3/4.txt
        """)
        s4.cli.cp('s4://bucket/cp/dst/', 'dst1/', recursive=True)
        assert run('grep ".*" $(find dst1/ -type f | sort)') == rm_whitespace("""
            dst1/1.txt:123
            dst1/2.txt:234
            dst1/3/4.txt:456
        """)
        s4.cli.cp('s4://bucket/cp/dst/', '.', recursive=True)
        assert run('grep ".*" $(find dst/ -type f | sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)
        run('rm -rf dst')
        s4.cli.cp('foo', 's4://bucket/cp/dst2', recursive=True)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/cp/dst2/'))) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/cp/dst2/', recursive=True))) == rm_whitespace("""
            _ _ _ cp/dst2/1.txt
            _ _ _ cp/dst2/2.txt
            _ _ _ cp/dst2/3/4.txt
        """)
        s4.cli.cp('s4://bucket/cp/dst', '.', recursive=True)
        assert run('grep ".*" $(find dst/ -type f | sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)

def test_ls():
    with servers():
        s4.cli.cp('/dev/null', 's4://bucket/other-listing/key0.txt')
        s4.cli.cp('/dev/null', 's4://bucket/listing/dir1/key1.txt')
        s4.cli.cp('/dev/null', 's4://bucket/listing/dir1/dir2/key2.txt')
        assert '\n'.join(s4.cli.ls('s4://bucket/listing/dir1/ke')) == rm_whitespace("""
            _ _ _ key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing/dir1/'))) == rm_whitespace("""
              PRE dir2/
            _ _ _ key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing/d'))) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing/'))) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing', recursive=True))) == rm_whitespace("""
            _ _ _ listing/dir1/dir2/key2.txt
            _ _ _ listing/dir1/key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing/', recursive=True))) == rm_whitespace("""
            _ _ _ listing/dir1/dir2/key2.txt
            _ _ _ listing/dir1/key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/listing/d', recursive=True))) == rm_whitespace("""
            _ _ _ listing/dir1/dir2/key2.txt
            _ _ _ listing/dir1/key1.txt
        """)
        with pytest.raises(SystemExit):
            s4.cli.ls('s4://bucket/fake/')

def test_rm():
    with servers():
        s4.cli.rm('s4://bucket/rm/di', recursive=True)
        s4.cli.cp('/dev/null', 's4://bucket/rm/dir1/key1.txt')
        s4.cli.cp('/dev/null', 's4://bucket/rm/dir1/dir2/key2.txt')
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/rm/', recursive=True))) == rm_whitespace("""
            _ _ _ rm/dir1/dir2/key2.txt
            _ _ _ rm/dir1/key1.txt
        """)
        s4.cli.rm('s4://bucket/rm/dir1/key1.txt')
        assert rm_whitespace('\n'.join(s4.cli.ls('s4://bucket/rm/', recursive=True))) == rm_whitespace("""
            _ _ _ rm/dir1/dir2/key2.txt
        """)
        s4.cli.rm('s4://bucket/rm/di', recursive=True)
        with pytest.raises(SystemExit):
            s4.cli.ls('s4://bucket/rm/', recursive=True)

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
