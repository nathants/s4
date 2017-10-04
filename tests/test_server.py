# use: boxed, for py.test --boxed
import contextlib
import itertools
import os
import pool.proc
import pytest
import requests
import s4.cli
import s4.server
import shell
import sys
import time
import util.log
import util.time
from shell import run

def rm_whitespace(x):
    return '\n'.join([y.strip()
                      for y in x.splitlines()
                      if y.strip()])

def start(port):
    with shell.cd(f'_{port}'):
        s4.http_port = lambda: port
        s4.server.start()

all_ports = itertools.count(8000)

@contextlib.contextmanager
def servers():
    util.log.setup()
    with util.time.timeout(3):
        with shell.tempdir():
            ports = [next(all_ports), next(all_ports), next(all_ports)]
            s4.servers = [('0.0.0.0', str(port)) for port in ports]
            s4._num_servers = len(s4.servers)
            procs = [pool.proc.new(start, port) for port in ports]
            watch = True
            def watcher():
                while watch:
                    for proc in procs:
                        if not proc.is_alive():
                            time.sleep(1)
                            sys.stdout.write('proc died! exiting...\n')
                            sys.stdout.flush()
                            os._exit(1)
            pool.thread.new(watcher)
            for _ in range(30):
                try:
                    for port in ports:
                        requests.get(f'http://0.0.0.0:{port}')
                    break
                except:
                    time.sleep(.1)
            else:
                assert False
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
        shell.run('cat out.txt') == "123"
        s4.cli.cp('s4://bucket/basic/dir/file2.txt', 'out2.txt')
        shell.run('cat out.txt') == "345\n"
        shell.run('mkdir foo/')
        s4.cli.cp('s4://bucket/basic/dir/file.txt', 'foo/')
        with open('foo/file.txt') as f:
            assert f.read() == "123"

def test_cp():
    with servers():
        s4.check_output('mkdir -p foo/3')
        with open('foo/1.txt', 'w') as f:
            f.write('123')
        with open('foo/2.txt', 'w') as f:
            f.write('234')
        with open('foo/3/4.txt', 'w') as f:
            f.write('456')
        s4.cli.cp('foo/', 's4://bucket/cp/dst/', recursive=True)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/cp/dst/'))) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/cp/dst/', recursive=True))) == rm_whitespace("""
            _ _ _ cp/dst/1.txt
            _ _ _ cp/dst/2.txt
            _ _ _ cp/dst/3/4.txt
        """)
        s4.cli.cp('s4://bucket/cp/dst/', 'dst1/', recursive=True)
        assert run('grep ".*" $(find dst1/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst1/1.txt:123
            dst1/2.txt:234
            dst1/3/4.txt:456
        """)
        s4.cli.cp('s4://bucket/cp/dst/', '.', recursive=True)
        assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)
        run('rm -rf dst')
        s4.cli.cp('foo', 's4://bucket/cp/dst2', recursive=True)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/cp/dst2/'))) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/cp/dst2/', recursive=True))) == rm_whitespace("""
            _ _ _ cp/dst2/1.txt
            _ _ _ cp/dst2/2.txt
            _ _ _ cp/dst2/3/4.txt
        """)
        s4.cli.cp('s4://bucket/cp/dst', '.', recursive=True)
        assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)

def test_listing():
    with servers():
        s4.cli.cp('/dev/null', 's4://bucket/listing/dir1/key1.txt')
        s4.cli.cp('/dev/null', 's4://bucket/listing/dir1/dir2/key2.txt')
        assert '\n'.join(s4.cli.ls('bucket/listing/dir1/ke')) == rm_whitespace("""
            _ _ _ key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/listing/dir1/'))) == rm_whitespace("""
              PRE dir2/
            _ _ _ key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/listing/d'))) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/listing/'))) == rm_whitespace("""
              PRE dir1/
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/listing/', recursive=True))) == rm_whitespace("""
            _ _ _ listing/dir1/dir2/key2.txt
            _ _ _ listing/dir1/key1.txt
        """)
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/listing/d', recursive=True))) == rm_whitespace("""
            _ _ _ listing/dir1/dir2/key2.txt
            _ _ _ listing/dir1/key1.txt
        """)
        with pytest.raises(AssertionError):
            s4.cli.ls('bucket/fake/')

def test_rm():
    with servers():
        s4.cli.rm('s4://bucket/rm/di', recursive=True)
        s4.cli.cp('/dev/null', 's4://bucket/rm/dir1/key1.txt')
        s4.cli.cp('/dev/null', 's4://bucket/rm/dir1/dir2/key2.txt')
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/rm/', recursive=True))) == rm_whitespace("""
            _ _ _ rm/dir1/dir2/key2.txt
            _ _ _ rm/dir1/key1.txt
        """)
        s4.cli.rm('s4://bucket/rm/dir1/key1.txt')
        assert rm_whitespace('\n'.join(s4.cli.ls('bucket/rm/', recursive=True))) == rm_whitespace("""
            _ _ _ rm/dir1/dir2/key2.txt
        """)
        s4.cli.rm('s4://bucket/rm/di', recursive=True)
        with pytest.raises(AssertionError):
            s4.cli.ls('bucket/rm/', recursive=True)
