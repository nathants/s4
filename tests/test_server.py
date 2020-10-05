import hashlib
import contextlib
import pytest
import logging
import os
import pool.proc
import requests
import shell
import time
import util.iter
import util.log
import util.time
import util.net
import uuid
from shell import run
from util.retry import retry

def setup_module():
    with shell.cd(os.path.dirname(os.path.abspath(__file__))):
        with shell.climb_git_root():
            shell.run('make -j', stream=True)
            os.environ['PATH'] += f':{os.getcwd()}/bin'

def rm_whitespace(x):
    return '\n'.join([y.strip()
                      for y in x.splitlines()
                      if y.strip()])

def start(port, conf):
    with shell.cd(f'_{port}'):
        for i in range(5):
            try:
                shell.run(f'timeout 60 s4-server -port {port} -conf {conf}', stream=True)
            except:
                logging.exception('')
                continue
        assert False, f'failed to start server on ports from: {port}'

@retry
def start_all():
    ports = [util.net.free_port() for _ in range(3)]
    conf = os.environ['S4_CONF_PATH'] = os.path.abspath(run('mktemp -p .'))
    with open(conf, 'w') as f:
        f.write('\n'.join(f'0.0.0.0:{port}' for port in ports) + '\n')
    procs = [pool.proc.new(start, port, conf) for port in ports]
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

def watcher(watch, procs):
    while True:
        for proc in procs:
            if not proc.is_alive():
                if not watch[0]:
                    return
                time.sleep(1)
                print('proc died! exiting...', flush=True)
                os._exit(1)

@contextlib.contextmanager
def servers(timeout=30):
    util.log.setup(format='%(message)s')
    shell.set['stream'] = True
    with util.time.timeout(timeout):
        with shell.stream():
            with shell.tempdir():
                procs = start_all()
                watch = [True]
                pool.thread.new(watcher, watch, procs)
                try:
                    yield
                finally:
                    watch[0] = False
                    for proc in procs:
                        proc.terminate()

def test_spaces_are_not_allowed():
    with servers():
        with pytest.raises(Exception):
            run('echo | s4 cp - "s4://bucket/basic/dir/fi le.txt"')

def test_updates_are_not_allowed():
    with servers():
        path = 's4://bucket/basic/dir/file.txt'
        run(f'echo | s4 cp - {path}')
        with pytest.raises(Exception):
            run(f'echo | s4 cp - {path}')
        run('timeout 3 s4 rm', path)
        run(f'echo | s4 cp - {path}')

def test_eval():
    with servers():
        run('echo 123 | s4 cp - s4://bucket/file.txt')
        assert '123' == run('s4 eval s4://bucket/file.txt "cat"')

def test_basic():
    with servers():
        run('echo 123 > file.txt')
        run('s4 cp file.txt s4://bucket/basic/dir/file.txt')
        run('echo 345 > file2.txt')
        run('s4 cp file2.txt s4://bucket/basic/dir/')
        assert run("s4 ls -r s4://bucket/ | awk '{print $NF}'").splitlines() == [
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
        assert run("s4 ls | awk '{print $NF}'").splitlines() == ['bucket']

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
        assert run("s4 ls -r s4://bucket | awk '{print $NF}'").splitlines() == [
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
        assert run("s4 ls -r s4://bucket | awk '{print $NF}'").splitlines() == [
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

def test_data_modifications_not_allowed():
    with servers():
        run('echo | s4 cp - s4://bucket/data.txt')
        path = run('find . -type f -name data.txt')
        assert path.endswith('/data.txt')
        with pytest.raises(Exception):
            run('echo >>', path)

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
        assert run("s4 ls -r s4://bucket/cp/dst/ | awk '{print $NF}'") == rm_whitespace("""
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
        assert rm_whitespace(run("s4 ls -r s4://bucket/cp/dst2/ | awk '{print $NF}'")) == rm_whitespace("""
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
        assert run("s4 ls s4://bucket/listing/dir1/ke | awk '{print $NF}'") == rm_whitespace("""
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
        assert rm_whitespace(run("s4 ls -r s4://bucket/listing | awk '{print $NF}'")) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        assert rm_whitespace(run("s4 ls -r s4://bucket/listing/ | awk '{print $NF}'")) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        assert rm_whitespace(run("s4 ls -r s4://bucket/listing/d | awk '{print $NF}'")) == rm_whitespace("""
            listing/dir1/dir2/key2.txt
            listing/dir1/key1.txt
        """)
        with pytest.raises(Exception):
            run('s4 ls s4://bucket/fake/')

def test_rm():
    with servers():
        run('echo | s4 cp - s4://bucket/rm/dir1/key1.txt')
        run('echo | s4 cp - s4://bucket/rm/dir1/dir2/key2.txt')
        assert rm_whitespace(run("s4 ls -r s4://bucket/rm/ | awk '{print $NF}'")) == rm_whitespace("""
            rm/dir1/dir2/key2.txt
            rm/dir1/key1.txt
        """)
        run('s4 rm s4://bucket/rm/dir1/key1.txt')
        assert rm_whitespace(run("s4 ls -r s4://bucket/rm/ | awk '{print $NF}'")) == rm_whitespace("""
            rm/dir1/dir2/key2.txt
        """)
        run('s4 rm -r s4://bucket/rm/di')
        with pytest.raises(Exception):
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

def test_map():
    with servers(1_000_000):
        src = 's4://bucket/data_in/'
        dst = 's4://bucket/data_out/'
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {src}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        assert run(f"s4 ls {src} | awk '{{print $NF}}' ").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {src}/00000 - | head -n5').splitlines() == ['Abelson', 'Aberdeen', 'Allison', 'Amsterdam', 'Apollos']
        run(f's4 map {src} {dst} "tr A-Z a-z"')
        assert run(f"s4 ls {dst} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {dst}/00000 - | head -n5').splitlines() == ['abelson', 'aberdeen', 'allison', 'amsterdam', 'apollos']
        run(f's4 cp -r {dst} result')
        assert run('cat result/*', stream=False) == '\n'.join(words).lower()

def test_map_glob():
    with servers(1_000_000):
        src = 's4://bucket/data_in/'
        dst = 's4://bucket/data_out/'
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {src}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        assert run(f"s4 ls {src} | awk '{{print $NF}}' ").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {src}/00000 - | head -n5').splitlines() == ['Abelson', 'Aberdeen', 'Allison', 'Amsterdam', 'Apollos']
        run(f's4 map {src}/*4 {dst} "tr A-Z a-z"')
        assert run(f"s4 ls {dst} | awk '{{print $NF}}'").splitlines() == ['00004']
        assert run(f's4 cp {dst}/00004 - | head -n5').splitlines() == ['mistreat', 'ml', 'modernize', 'modishly', 'molars']

# utils for map tests
with open('/tmp/bucket.py', 'w') as f:
    f.write("""
import hashlib
import sys
num_buckets = int(sys.argv[1])
for line in sys.stdin:
    cols = line.rstrip().split(',')
    hash_bytes = hashlib.md5(cols[0].encode()).digest()
    hash_int = int.from_bytes(hash_bytes, 'big')
    bucket = hash_int % num_buckets
    cols = [str(bucket).rjust(5, '0')] + cols
    print(','.join(cols))
""")

# utils for map tests
with open('/tmp/partition.py', 'w') as f:
    f.write(r"""
import sys
import collections
num_buckets = int(sys.argv[1])
files = {}
for line in sys.stdin:
    line = line.rstrip()
    bucket, *cols = line.split(',')
    if bucket not in files:
        files[bucket] = open(bucket, "w")
    files[bucket].write(','.join(cols) + '\n')
for name, file in files.items():
    print(name)
    file.close()
""")

def test_map_to_n():
    # build on map test
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/' # bucketed
        step3 = 's4://bucket/step3/' # partitioned
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        assert run(f"s4 ls {step1} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        run(f's4 map {step1} {step2} "python3 /tmp/bucket.py 3"')
        assert run(f"s4 ls {step2} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {step2}/00000 - | head -n5').splitlines() == ['00000,Abelson', '00000,Aberdeen', '00002,Allison', '00001,Amsterdam', '00002,Apollos']
        run(f's4 map-to-n {step2} {step3} "python3 /tmp/partition.py 3"')
        assert run(f"s4 ls -r {step3} | awk '{{print $NF}}'").splitlines() == [
            'step3/00000/00000', # $outdir/$file_num/$bucket_num
            'step3/00000/00001',
            'step3/00000/00002',
            'step3/00001/00000',
            'step3/00001/00001',
            'step3/00001/00002',
            'step3/00002/00000',
            'step3/00002/00001',
            'step3/00002/00002',
            'step3/00003/00000',
            'step3/00003/00001',
            'step3/00003/00002',
            'step3/00004/00000',
            'step3/00004/00001',
            'step3/00004/00002',
            'step3/00005/00000',
            'step3/00005/00001',
            'step3/00005/00002',
        ]
        run(f's4 cp -r {step3} step3/')
        result = []
        num_buckets = 3
        for word in words:
            hash_bytes = hashlib.md5(word.encode()).digest()
            hash_int = int.from_bytes(hash_bytes, 'big')
            bucket = hash_int % num_buckets
            if bucket == 0:
                result.append(word)
        assert '\n'.join(result) == run('cat step3/*/00000', stream=False)

def test_map_to_n_can_result_in_zero_files():
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/'
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        run(f's4 map-to-n {step1} {step2} "cat >/dev/null && echo"')
        with pytest.raises(Exception):
            run(f's4 ls -r {step2}')

def test_map_to_n_should_fail_quickly_on_bad_file_paths():
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/'
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        with pytest.raises(Exception):
            run(f's4 map-to-n {step1} {step2} "cat >/dev/null && echo does_not_exist"')

def test_map_from_n():
    # builds on map and map_to_n test
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/' # bucketed
        step3 = 's4://bucket/step3/' # partitioned
        step4 = 's4://bucket/step4/' # merged buckets
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        assert run(f"s4 ls {step1} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        run(f's4 map {step1} {step2} "python3 /tmp/bucket.py 3"')
        assert run(f"s4 ls {step2} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {step2}/00000 - | head -n5').splitlines() == ['00000,Abelson', '00000,Aberdeen', '00002,Allison', '00001,Amsterdam', '00002,Apollos']
        run(f's4 map-to-n {step2} {step3} "python3 /tmp/partition.py 3"')
        run(f"s4 map-from-n {step3} {step4} 'xargs cat'")
        assert run(f"s4 ls -r {step4} | awk '{{print $NF}}'").splitlines() == [
            'step4/00000',
            'step4/00001',
            'step4/00002',
        ]
        run(f's4 cp -r {step4} step4/')
        result = []
        num_buckets = 3
        for word in words:
            hash_bytes = hashlib.md5(word.encode()).digest()
            hash_int = int.from_bytes(hash_bytes, 'big')
            bucket = hash_int % num_buckets
            if bucket == 0:
                result.append(word)
        assert sorted(result) == sorted(run('cat step4/00000', stream=False).splitlines())

def test_map_from_n_without_numeric_prefixes():
    # builds on map and map_to_n test
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/' # bucketed
        step3 = 's4://bucket/step3/' # partitioned
        step4 = 's4://bucket/step4/' # merged buckets
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{uuid.uuid4()}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        run(f's4 map {step1} {step2} "python3 /tmp/bucket.py 3"')
        run(f's4 map-to-n {step2} {step3} "python3 /tmp/partition.py 3"')
        run(f"s4 map-from-n {step3} {step4} 'xargs cat'")
        assert run(f"s4 ls -r {step4} | awk '{{print $NF}}'").splitlines() == [
            'step4/00000',
            'step4/00001',
            'step4/00002',
        ]
        run(f's4 cp -r {step4} step4/')
        result = []
        num_buckets = 3
        for word in words:
            hash_bytes = hashlib.md5(word.encode()).digest()
            hash_int = int.from_bytes(hash_bytes, 'big')
            bucket = hash_int % num_buckets
            if bucket == 0:
                result.append(word)
        assert sorted(result) == sorted(run('cat step4/00000', stream=False).splitlines())

def test_map_should_work_on_the_output_of_map_to_n():
    with servers(1_000_000):
        step1 = 's4://bucket/step1/' # input data
        step2 = 's4://bucket/step2/' # bucketed
        step3 = 's4://bucket/step3/' # partitioned
        step4 = 's4://bucket/step4/' # mapped partitioned
        step5 = 's4://bucket/step5/' # merged buckets
        def fn(arg):
            i, chunk = arg
            run(f's4 cp - {step1}{i:05}', stdin="\n".join(chunk) + "\n")
        list(pool.thread.map(fn, enumerate(util.iter.chunk(words, 180))))
        assert run(f"s4 ls {step1} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        run(f's4 map {step1} {step2} "python3 /tmp/bucket.py 3"')
        assert run(f"s4 ls {step2} | awk '{{print $NF}}'").splitlines() == ['00000', '00001', '00002', '00003', '00004', '00005']
        assert run(f's4 cp {step2}/00000 - | head -n5').splitlines() == ['00000,Abelson', '00000,Aberdeen', '00002,Allison', '00001,Amsterdam', '00002,Apollos']
        run(f's4 map-to-n {step2} {step3} "python3 /tmp/partition.py 3"')
        ## map is recursive, so it can work on the output of map-to-n
        run(f"s4 map {step3} {step4} 'while read row; do echo $(echo $row | head -c4); done'")
        ##
        run(f"s4 map-from-n {step4} {step5} 'xargs cat'")
        assert run(f"s4 ls -r {step5} | awk '{{print $NF}}'").splitlines() == [
            'step5/00000',
            'step5/00001',
            'step5/00002',
        ]
        run(f's4 cp -r {step5} step5/')
        result = []
        num_buckets = 3
        for word in words:
            hash_bytes = hashlib.md5(word.encode()).digest()
            hash_int = int.from_bytes(hash_bytes, 'big')
            bucket = hash_int % num_buckets
            if bucket == 0:
                result.append(word[:4])
        assert result == run('cat step5/00000', stream=False).splitlines()

def test_map_should_preserve_suffix():
    with servers(1_000_000):
        run('echo | s4 cp - s4://step1/000_key0')
        run('s4 map s4://step1/ s4://step2/ "cat"')
        assert '000_key0' == run("s4 ls s4://step2/ | awk '{print $NF}'")

def test_map_from_n_should_work_on_deep_directories_and_preserve_suffix():
    with servers(1_000_000):
        run('s4 cp - s4://step1/may/000_key0/000_bucket0', stdin="1\n")
        run('s4 cp - s4://step1/may/001_key1/000_bucket0', stdin="2\n")
        run('s4 cp - s4://step1/jun/000_key0/000_bucket0', stdin="3\n")
        run('s4 cp - s4://step1/jun/001_key1/000_bucket0', stdin="4\n")
        run('s4 cp - s4://step1/jul/000_key0/000_bucket0', stdin="5\n")
        run('s4 cp - s4://step1/jul/001_key1/000_bucket0', stdin="6\n")
        run("s4 map-from-n s4://step1/ s4://step2/ 'grep -e may -e jun | xargs cat | sort -n'")
        assert '1\n2\n3\n4' == run('s4 eval s4://step2/000_bucket0 "cat"')

words = [
    "Abelson", "Aberdeen", "Allison", "Amsterdam", "Apollos", "Arabian", "Assad", "Austerlitz", "Bactria", "Baldwin", "Belinda", "Bethe", "Blondel",
    "Bobbitt", "Boone", "Bowery", "Browne", "Candy", "Carmella", "Cheever", "Chicano", "Christa", "Clyde", "Conakry", "Cotopaxi", "Dalai",
    "Damian", "Davidson", "Deana", "Dobro", "Dona", "Doritos", "Drew", "Eggo", "Elmer", "Eunice", "Everett", "Fauntleroy", "Fortaleza",
    "Frenchwoman", "Freudian", "Galatea", "Grenoble", "Gwendoline", "Hals", "Hastings", "Head", "Hilda", "Hoff", "Hohenzollern", "Hosea", "Internet",
    "Lebanese", "Leroy", "Lieberman", "Louisville", "Loyang", "Loyola", "Lubavitcher", "Luke", "Luxembourger", "Macao", "Madeleine", "Maghreb", "Magus",
    "Iranian", "Irene", "Israelis", "Jacobin", "Jansenist", "Jewishness", "Jorge", "Joy", "Judaeo", "Kaye", "Knuths", "Laval", "Leanna",
    "Maidenform", "Malabo", "Marissa", "Matthews", "Mauriac", "Mauritius", "Mauro", "Milne", "Mississippian", "Muscat", "NEH", "NFC", "Natalie",
    "Nellie", "Norman", "Novokuznetsk", "Olaf", "Ono", "PO", "Pittsburgh", "Presbyterianism", "Procrustean", "Proust", "Pugh", "Quixotism", "Rapunzel",
    "Rochester", "Rodrigo", "Schnabel", "Selectric", "Shavuot", "TARP", "Terrell", "Tony", "Topeka", "Tunisia", "Turner", "Ulysses", "Utah",
    "Valarie", "Veracruz", "Volta", "WTO", "Wallenstein", "Waring", "Woolf", "YMHA", "Yunnan", "Zedekiah", "Zoroastrians", "Zubeneschamali", "abhorred",
    "abnormal", "aborning", "abracadabra", "abscissa", "academic", "accelerating", "acculturates", "acquaintanceship", "acquiescing", "acupressure", "add", "addicts", "addling",
    "adjoins", "adulterants", "aerialists", "aerodromes", "affirmatives", "aftershaves", "afterthought", "aggravate", "airguns", "alewives", "alighting", "allies", "alpines",
    "public", "puffier", "putsch", "quacked", "quadruplication", "quads", "quandary", "queered", "questioning", "quicksilver", "quoted", "radiate", "razor",
    "reappear", "reassigned", "reburied", "receptively", "recessives", "recipe", "reclassification", "recluses", "reconciliation", "recurrently", "redden", "redrafts", "redskin",
    "reduce", "reemphasizes", "reenacting", "refinement", "reforging", "reformations", "refurbished", "regard", "regenerate", "regretted", "rejoices", "relaxing", "remarriages",
    "rematch", "reminders", "renovation", "rephrasing", "repleted", "reprehensibly", "res", "reside", "responsively", "restated", "restoratives", "resultants", "retaught",
    "rotter", "ruggeder", "russet", "safflower", "sanctimony", "sandblaster", "sassafras", "sayings", "scapegraces", "scatology", "schnapps", "scintillating", "scourges",
    "retype", "revenue", "rhombuses", "rigmarole", "rigor", "rinsed", "rissole", "roaster", "robocalls", "rogering", "roles", "rooming", "rosemary",
    "scrams", "scrappiest", "screenshots", "scuffle", "sculleries", "seethes", "selloff", "sensualists", "sentimentalize", "sentimentalizing", "sexually", "sh", "shading",
    "shadowing", "shakiness", "shamelessness", "shareholdings", "shatters", "shearer", "shepherdess", "shits", "shorebird", "shutoff", "sibylline", "sierra", "silencing",
    "sitarists", "skillet", "slappers", "sleeps", "sleeved", "sliminess", "smack", "societies", "society", "sofas", "softened", "solemnly", "soliciting",
    "alumnus", "ambidextrously", "ambled", "andirons", "antedating", "antics", "anxieties", "aphorisms", "appeasement", "appraisal", "arbitrating", "architect", "architectonics",
    "averaging", "awhile", "babying", "backaches", "backstopped", "baker", "balancing", "balustrade", "bandmaster", "banning", "baptismal", "bareheaded", "beauts",
    "commissions", "commutation", "competitively", "concealable", "conclusiveness", "confidentially", "confiscating", "conjurers", "conscientious", "continuing", "contractually", "converging", "convey",
    "cooling", "copulates", "cornrow", "corrugating", "cosigners", "cosmogonies", "cosmologists", "cosplay", "counseled", "countertenors", "crackle", "craving", "crayoning",
    "dandier", "dangerous", "darn", "dauntless", "dc", "deactivation", "dearest", "deceased", "decisiveness", "decomposes", "decrement", "defenestrations", "dehydrators",
    "crazes", "creams", "credibly", "crookneck", "crowds", "cudgelings", "culminations", "cures", "curling", "curvaceous", "cusp", "cutaways", "cutting",
    "delete", "dementia", "demonstratively", "depilatory", "deployment", "depolarizes", "deposed", "desensitized", "desiccates", "detergent", "devote", "diaphragms", "digesting",
    "dilate", "disastrously", "disencumber", "disfavored", "divides", "dobs", "docility", "dogfights", "doggedness", "domestic", "domesticate", "dongs", "dots",
    "doughnuts", "downing", "downspout", "dreamers", "dredging", "dribblers", "dubbed", "dun", "duration", "earpiece", "easing", "effectively", "effendis",
    "eggbeaters", "electable", "elimination", "elusively", "enabler", "enchantingly", "enciphered", "enciphering", "enervation", "enormity", "entangle", "enthralls", "enthuses",
    "entrap", "environment", "epigram", "epigraphy", "equalize", "equalizing", "equidistantly", "equipment", "equivocated", "erroneous", "escalloped", "esplanades", "eulogistic",
    "evacuate", "evils", "examiner", "expropriator", "extremest", "extrinsic", "falsified", "fancied", "fastbacks", "fathomless", "featherweights", "feds", "feminines",
    "fermium", "ferule", "fictitious", "filer", "filigrees", "firescreen", "fixing", "flatbread", "fleeting", "fleshlier", "flexing", "flickering", "flusher",
    "foretasted", "forevermore", "forwarding", "forwardness", "fossilize", "freeloads", "fretfulness", "frogman", "fuchsias", "fulfillment", "furthering", "gadgets", "galumphing",
    "galumphs", "game", "gamuts", "garroted", "gaze", "gazumped", "genes", "gentlefolk", "geodes", "geographers", "ghosting", "gimbals", "gloatingly",
    "goldbrickers", "goslings", "gouaches", "gracefulness", "grafts", "granddaughter", "grandiloquence", "granges", "grater", "grebes", "gregariousness", "grenadiers", "groping",
    "grosgrain", "grovelled", "guardrooms", "guesstimated", "gussied", "gusting", "gymnasts", "gyps", "haggard", "harmlessly", "harrow", "haste", "hatreds",
    "armband", "arsed", "asininity", "assessments", "assizes", "astigmatism", "astringency", "astronomers", "attains", "aunt", "auricular", "authentications", "autocratic",
    "bedeviling", "beholders", "being", "belling", "bellyache", "bemoan", "beseecher", "besieged", "bespatters", "bethinking", "biggie", "biker", "biologist",
    "bipartisanship", "bivouac", "blackened", "bldg", "blistery", "bloodthirsty", "bludgeons", "boating", "bobbies", "bodacious", "bodies", "boniest", "bootlace",
    "bossing", "bouncily", "bowdlerizes", "bowlegs", "braes", "braggart", "brakes", "branching", "brazer", "breastfeed", "breather", "briefcase", "briefcases",
    "briefed", "brilliantly", "brotherhood", "broths", "browbeaten", "bucks", "builtin", "bully", "bunch", "burgomaster", "bursitis", "bursting", "bushes",
    "buyouts", "caber", "cafetieres", "caiman", "callus", "cankered", "cannibalizes", "canvasbacks", "capstan", "cardamom", "caregiver", "carouse", "catastrophe",
    "catatonic", "cavils", "cellulose", "certificated", "chambermaid", "channelizing", "chapeaus", "chappy", "charms", "chastisement", "chummed", "churchwoman", "ciceroni",
    "circuitry", "circumlocutory", "circumstantially", "classically", "clientele", "clomps", "clunkiest", "codger", "cods", "cogitated", "collaborator", "columbines", "comings",
    "headstones", "headword", "heartland", "heavy", "hellholes", "helpfully", "hindmost", "hoer", "holdover", "holes", "homewreckers", "homicides", "honeylocust",
    "hypoglycemic", "imbibers", "immunoglobulin", "impertinence", "impolite", "impolitic", "impressionists", "impulsively", "inappreciably", "incarnate", "incomprehensibly", "indebted", "indicted",
    "horrendously", "hospitalization", "hostels", "however", "howls", "huddling", "hues", "human", "humanized", "humbler", "hushing", "hybridization", "hymnbooks",
    "indissolubility", "inducted", "indulge", "inglenooks", "inheriting", "inhibitions", "inmate", "inner", "innocently", "innocuously", "inquisitors", "insecticide", "inserts",
    "inspiration", "institutional", "intent", "intermarry", "intriguer", "inveighs", "invertebrates", "inveterate", "inwards", "ironies", "isle", "iterated", "jobbing",
    "jolts", "journalese", "junker", "junking", "kale", "king", "kinkiness", "kissogram", "kluged", "laborious", "ladyships", "lancing", "lapped",
    "lapwings", "larcenist", "lawns", "laxity", "leafage", "led", "lenses", "lexers", "linebackers", "linoleum", "lint", "liquidizes", "livid",
    "locate", "lofty", "loggerheads", "lummoxes", "lunching", "macaronis", "magnificently", "maids", "mainstreamed", "maledictions", "mallow", "mangled", "manhandling",
    "mannequins", "mantis", "markka", "meagerly", "meanness", "meas", "meddlers", "mendicants", "microprocessor", "migrant", "mikados", "militarists", "miming",
    "miner", "miscommunications", "miscount", "misinterpret", "mistaken", "mistreat", "ml", "modernize", "modishly", "molars", "monaural", "moneymaker", "moneys",
    "monosyllabic", "moralizers", "more", "mountains", "mounted", "mowers", "mussier", "naturals", "necromancy", "neglecting", "nettle", "neutralizing", "newscasters",
    "objurgate", "objurgations", "occlusion", "odd", "officeholder", "oiliest", "onside", "onsite", "oomph", "oozier", "opticians", "optimizes", "orgy",
    "nibbles", "nincompoop", "nonfictional", "noninflationary", "noninvasive", "nonthreatening", "nonunion", "novices", "null", "numerologist", "nutshells", "oak", "oath",
    "osteoporosis", "outfield", "outlaws", "outlooks", "outstayed", "overate", "overcasts", "overcautious", "overdressing", "overfeeds", "overindulgence", "overlie", "overproduced",
    "overran", "pager", "paradox", "parches", "paroles", "partnered", "pasha", "patisseries", "patriarchs", "patronage", "peculator", "peculiarly", "peduncle",
    "peepbo", "pensively", "perfectas", "peripherals", "peritoneal", "perm", "persevering", "persisting", "persuasions", "petitioners", "pharmacology", "phasing", "phenol",
    "phenotype", "photocopying", "photostat", "physiognomies", "picks", "picnicking", "pie", "pilchards", "pinheads", "pitching", "plagues", "planned", "plantain",
    "playgroup", "pleasures", "plonk", "plunders", "poi", "pollinated", "popularize", "population", "postilion", "pother", "potpies", "potter", "pounces",
    "poundage", "praying", "precursor", "precursory", "presorting", "pressures", "prestige", "prevaricating", "previewers", "prickle", "private", "probabilistic", "procedural",
    "procurers", "prodigious", "progeny", "proliferates", "prolongation", "proselytizes", "protective", "protoplasmic", "provender", "provocative", "pseudonym", "pshaws", "psychological",
    "solitude", "soloists", "sols", "south", "sparring", "spewed", "sphinxes", "spiderweb", "spindliest", "spiritedly", "splays", "spuds", "stables",
    "staggeringly", "staphylococci", "starchy", "starved", "starves", "statehood", "statutory", "stets", "stigmatized", "stilettos", "stomacher", "strafe", "stratagems",
    "strategies", "stressing", "stringed", "stripling", "strongly", "subconsciously", "subcontracting", "subcontractors", "subdomains", "subliminal", "subroutines", "subverting", "succubi",
    "suffocated", "supermarket", "surged", "surlier", "surreys", "sussing", "swaddles", "swaddling", "switching", "syncopation", "t", "tachyon", "tactfully",
    "thickos", "this", "tho", "thoroughbreds", "ticktacktoe", "tinctured", "tintinnabulation", "titillatingly", "toastier", "tonsillectomy", "touchdown", "town", "towrope",
    "tactlessness", "tailbacks", "tamales", "teaser", "tellingly", "temped", "tenoned", "tenterhooks", "tepee", "tethered", "theosophic", "therapeutic", "thermodynamic",
    "traction", "transferable", "transgenders", "transgenic", "trifle", "tundras", "twenty", "typing", "ulcerate", "unbosoms", "unbuttoning", "underclassman", "undertake",
    "underutilized", "undesired", "unfrock", "ungainlier", "unhesitating", "unknowable", "unloveliest", "unmanageable", "unrecognized", "unreliability", "unsportsmanlike", "uppercase", "utilizes",
    "vaccinations", "vamoose", "vapidity", "vaults", "veep", "venturesomeness", "vertex", "vest", "vestibule", "voyeur", "vulgarian", "wakes", "wangling",
    "washcloths", "wassailed", "watchful", "waterfalls", "weekender", "wheedling", "wherein", "whopping", "wildlife", "wilier", "windowpanes", "wingnuts", "wisest",
    "wogs", "woodiest", "woodlands", "woodworking", "woulds", "wrinklier", "wrongly", "yammered", "yest", "zebus", "zeitgeists", "zings",
]
