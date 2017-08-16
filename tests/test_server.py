import uuid
import pytest
import shell
import os
os.environ['s3_stubbed_session'] = str(uuid.uuid4())

import aws.s3_stubbed as s3
from shell import run

preamble = 'python3 -c "from aws.s3_stubbed import main; main()"'

def rm_whitespace(x):
    return '\n'.join([y.strip()
                      for y in x.splitlines()
                      if y.strip()])

def test_basic():
    with shell.tempdir():
        with open('input.txt', 'w') as f:
            f.write('123')
        run(preamble, 'cp input.txt s3://bucket/basic/dir/file.txt')
        run('echo asdf |', preamble, 'cp - s3://bucket/basic/dir/stdin.txt')
        assert run(preamble, 'ls s3://bucket/ --recursive').splitlines() == [
            '_ _ _ basic/dir/file.txt',
            '_ _ _ basic/dir/stdin.txt']
        assert run(preamble, 'cp s3://bucket/basic/dir/file.txt -') == "123"
        assert run(preamble, 'cp s3://bucket/basic/dir/stdin.txt -') == "asdf"
        run(preamble, 'cp s3://bucket/basic/dir/file.txt file.downloaded')
        with open('file.downloaded') as f:
            assert f.read() == "123"
        run(preamble, 'cp s3://bucket/basic/dir/stdin.txt stdin.downloaded')
        with open('stdin.downloaded') as f:
            assert f.read() == "asdf\n"
        run("mkdir foo")
        run(preamble, 'cp s3://bucket/basic/dir/stdin.txt foo/', stream=True)
        with open('foo/stdin.txt') as f:
            assert f.read() == "asdf\n"

def test_cp_s3_to_s3():
    run('echo asdf |', preamble, 'cp - s3://bucket/s3_to_s3/a.txt')
    run(preamble, 'cp s3://bucket/s3_to_s3/a.txt s3://bucket/s3_to_s3/b.txt')
    assert run(preamble, 'cp s3://bucket/s3_to_s3/b.txt -') == "asdf"
    assert run(preamble, 'ls s3://bucket/s3_to_s3/').splitlines() == [
        '_ _ _ a.txt',
        '_ _ _ b.txt',
    ]

def test_cp():
    with shell.tempdir():
        run('mkdir -p foo/3')
        with open('foo/1.txt', 'w') as f:
            f.write('123')
        with open('foo/2.txt', 'w') as f:
            f.write('234')
        with open('foo/3/4.txt', 'w') as f:
            f.write('456')
        run(preamble, 'cp foo/ s3://bucket/cp/dst/ --recursive')
        assert rm_whitespace(run(preamble, 'ls bucket/cp/dst/')) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace(run(preamble, 'ls bucket/cp/dst/ --recursive')) == rm_whitespace("""
            _ _ _ cp/dst/1.txt
            _ _ _ cp/dst/2.txt
            _ _ _ cp/dst/3/4.txt
        """)
        run(preamble, 'cp s3://bucket/cp/dst/ dst1/ --recursive')
        assert run('grep ".*" $(find dst1/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst1/1.txt:123
            dst1/2.txt:234
            dst1/3/4.txt:456
        """)
        run(preamble, 'cp s3://bucket/cp/dst/ . --recursive')
        assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)
        run('rm -rf dst')
        run(preamble, 'cp foo s3://bucket/cp/dst2 --recursive')
        assert rm_whitespace(run(preamble, 'ls bucket/cp/dst2/')) == rm_whitespace("""
              PRE 3/
            _ _ _ 1.txt
            _ _ _ 2.txt
        """)
        assert rm_whitespace(run(preamble, 'ls bucket/cp/dst2/ --recursive')) == rm_whitespace("""
            _ _ _ cp/dst2/1.txt
            _ _ _ cp/dst2/2.txt
            _ _ _ cp/dst2/3/4.txt
        """)
        run(preamble, 'cp s3://bucket/cp/dst . --recursive')
        assert run('grep ".*" $(find dst/ -type f|LC_ALL=C sort)') == rm_whitespace("""
            dst/1.txt:123
            dst/2.txt:234
            dst/3/4.txt:456
        """)

def test_listing():
    run('echo |', preamble, 'cp - s3://bucket/listing/dir1/key1.txt')
    run('echo |', preamble, 'cp - s3://bucket/listing/dir1/dir2/key2.txt')
    assert run(preamble, 'ls bucket/listing/dir1/ke') == rm_whitespace("""
        _ _ _ key1.txt
    """)
    assert rm_whitespace(run(preamble, 'ls bucket/listing/dir1/')) == rm_whitespace("""
          PRE dir2/
        _ _ _ key1.txt
    """)
    assert rm_whitespace(run(preamble, 'ls bucket/listing/d')) == rm_whitespace("""
          PRE dir1/
    """)
    assert rm_whitespace(run(preamble, 'ls bucket/listing/d --recursive')) == rm_whitespace("""
        _ _ _ listing/dir1/dir2/key2.txt
        _ _ _ listing/dir1/key1.txt
    """)
    with pytest.raises(AssertionError):
        run(preamble, 'ls bucket/fake/')

def test_rm():
    run(preamble, 'rm s3://bucket/rm/di --recursive')
    run('echo |', preamble, 'cp - s3://bucket/rm/dir1/key1.txt')
    run('echo |', preamble, 'cp - s3://bucket/rm/dir1/dir2/key2.txt')
    assert rm_whitespace(run(preamble, 'ls bucket/rm/ --recursive')) == rm_whitespace("""
        _ _ _ rm/dir1/dir2/key2.txt
        _ _ _ rm/dir1/key1.txt
    """)
    run(preamble, 'rm s3://bucket/rm/dir1/key1.txt')
    assert rm_whitespace(run(preamble, 'ls bucket/rm/ --recursive')) == rm_whitespace("""
        _ _ _ rm/dir1/dir2/key2.txt
    """)
    run(preamble, 'rm s3://bucket/rm/di --recursive')
    assert rm_whitespace(run(preamble, 'ls bucket/rm/ --recursive')) == ''

def test_prefixes():
    assert ["", "a/", "a/b/", "a/b/c/"] == s3._prefixes('a/b/c/d.csv')

def test_binary():
    with shell.tempdir():
        with open('1.txt', 'w') as f:
            f.write('123')
        run('cat 1.txt | lz4 -1 |', preamble, 'cp - s3://bucket/binary/1.txt')
        assert '123' == run(preamble, 'cp s3://bucket/binary/1.txt - | lz4 -d -c')
