import uuid
import util.time
import sys
import time
import requests
import pool.proc
import s4.server
import shell
import os
import contextlib
import s4.cli



def rm_whitespace(x):
    return '\n'.join([y.strip()
                      for y in x.splitlines()
                      if y.strip()])

def start(port):
    with shell.cd(f'_{port}'):
        s4.server.start(port)

@contextlib.contextmanager
def servers():
    with util.time.timeout(5):
        with shell.tempdir():
            ports = [8000, 8001, 8002]
            # ports = [8000]
            s4.servers = [('0.0.0.0', str(port)) for port in ports]
            s4._num_servers = len(s4.servers)
            s4.http_port = ports[0]
            procs = [pool.proc.new(start, port) for port in ports]
            watch = True
            def watcher():
                while watch:
                    for proc in procs:
                        if not proc.is_alive():
                            sys.stdout.write('proc died!\n')
                            sys.stdout.flush()
                            import time
                            time.sleep(1)
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
        s4.cli.cp('file.txt', 's3://bucket/basic/dir/file.txt')
        with open('file2.txt', 'w') as f:
            f.write('345\n')
        s4.cli.cp('file2.txt', 's3://bucket/basic/dir/')
        # assert s4.cli.ls('s3://bucket/', recursive=True) == [
        #     '_ _ _ basic/dir/file.txt',
        #     '_ _ _ basic/dir/file2.txt',
        # ]
        # s4.cli.cp('s3://bucket/basic/dir/file.txt', 'out.txt')
        # shell.run('cat out.txt') == "123"
        # s4.cli.cp('s3://bucket/basic/dir/file2.txt', 'out2.txt')
        # shell.run('cat out.txt') == "345\n"
        # shell.run('mkdir foo/')
        # s4.cli.cp('s3://bucket/basic/dir/file.txt', 'foo/')
        # with open('foo/file.txt') as f:
        #     assert f.read() == "123"

# def test_cp_s3_to_s3():
#     run('echo asdf |', preamble, 'cp - s3://bucket/s3_to_s3/a.txt')
#     run(preamble, 'cp s3://bucket/s3_to_s3/a.txt s3://bucket/s3_to_s3/b.txt')
#     assert run(preamble, 'cp s3://bucket/s3_to_s3/b.txt -') == "asdf"
#     assert run(preamble, 'ls s3://bucket/s3_to_s3/').splitlines() == [
#         '_ _ _ a.txt',
#         '_ _ _ b.txt',
#     ]

def test_cp():
    pass

    # with servers():
    #     run('mkdir -p foo/3')
    #     with open('foo/1.txt', 'w') as f:
    #         f.write('123')
    #     with open('foo/2.txt', 'w') as f:
    #         f.write('234')
    #     with open('foo/3/4.txt', 'w') as f:
    #         f.write('456')
    #     s4.cli.cp('foo/', 's3://bucket/cp/dst/', recursive=True)
    #     assert '\n'.join(s4.cli.ls('bucket/cp/dst/')) == rm_whitespace("""
    #           PRE 3/
    #         _ _ _ 1.txt
    #         _ _ _ 2.txt
    #     """)

        # assert rm_whitespace(s4.cli.ls('bucket/cp/dst/', recursive=True)) == rm_whitespace("""
        #     _ _ _ cp/dst/1.txt
        #     _ _ _ cp/dst/2.txt
        #     _ _ _ cp/dst/3/4.txt
        # """)
        # s4.cli.cp('s3://bucket/cp/dst/', 'dst1/', recursive=True)
        # assert run('grep ".*" $(find dst1/ -type f|LC_ALL=C sort)') == rm_whitespace("""
        #     dst1/1.txt:123
        #     dst1/2.txt:234
        #     dst1/3/4.txt:456
        # """)
        # s4.cli.cp('s3://bucket/cp/dst/', '.', recursive=True)
        # assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
        #     dst/1.txt:123
        #     dst/2.txt:234
        #     dst/3/4.txt:456
        # """)
        # run('rm -rf dst')
        # s4.cli.cp('foo', 's3://bucket/cp/dst2', recursive=True)
        # assert rm_whitespace(s4.cli.ls('bucket/cp/dst2/')) == rm_whitespace("""
        #       PRE 3/
        #     _ _ _ 1.txt
        #     _ _ _ 2.txt
        # """)
        # assert rm_whitespace(s4.cli.ls('bucket/cp/dst2/', recursive=True)) == rm_whitespace("""
        #     _ _ _ cp/dst2/1.txt
        #     _ _ _ cp/dst2/2.txt
        #     _ _ _ cp/dst2/3/4.txt
        # """)
        # s4.cli.cp('s3://bucket/cp/dst', '.', recursive=True)
        # assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
        #     dst/1.txt:123
        #     dst/2.txt:234
        #     dst/3/4.txt:456
        # """)

# def test_listing():
#     run('echo |', preamble, 'cp - s3://bucket/listing/dir1/key1.txt')
#     run('echo |', preamble, 'cp - s3://bucket/listing/dir1/dir2/key2.txt')
#     assert s4.cli.ls('bucket/listing/dir1/ke') == rm_whitespace("""
#         _ _ _ key1.txt
#     """)
#     assert rm_whitespace(s4.cli.ls('bucket/listing/dir1/')) == rm_whitespace("""
#           PRE dir2/
#         _ _ _ key1.txt
#     """)
#     assert rm_whitespace(s4.cli.ls('bucket/listing/d')) == rm_whitespace("""
#           PRE dir1/
#     """)
#     assert rm_whitespace(s4.cli.ls('bucket/listing/d --recursive')) == rm_whitespace("""
#         _ _ _ listing/dir1/dir2/key2.txt
#         _ _ _ listing/dir1/key1.txt
#     """)
#     with pytest.raises(AssertionError):
#         s4.cli.ls('bucket/fake/')

# def test_rm():
#     s4.cli.rm('s3://bucket/rm/di --recursive')
#     run('echo |', preamble, 'cp - s3://bucket/rm/dir1/key1.txt')
#     run('echo |', preamble, 'cp - s3://bucket/rm/dir1/dir2/key2.txt')
#     assert rm_whitespace(s4.cli.ls('bucket/rm/ --recursive')) == rm_whitespace("""
#         _ _ _ rm/dir1/dir2/key2.txt
#         _ _ _ rm/dir1/key1.txt
#     """)
#     s4.cli.rm('s3://bucket/rm/dir1/key1.txt')
#     assert rm_whitespace(s4.cli.ls('bucket/rm/ --recursive')) == rm_whitespace("""
#         _ _ _ rm/dir1/dir2/key2.txt
#     """)
#     s4.cli.rm('s3://bucket/rm/di --recursive')
#     assert rm_whitespace(s4.cli.ls('bucket/rm/ --recursive')) == ''

# def test_prefixes():
#     assert ["", "a/", "a/b/", "a/b/c/"] == s3._prefixes('a/b/c/d.csv')

# def test_binary():
#     with shell.tempdir():
#         with open('1.txt', 'w') as f:
#             f.write('123')
#         run('cat 1.txt | lz4 -1 |', preamble, 'cp - s3://bucket/binary/1.txt')
#         assert '123' == s4.cli.cp('s3://bucket/binary/1.txt - | lz4 -d -c')
