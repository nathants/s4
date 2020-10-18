import s4.cli

def test_globs():
    indir, glob = s4.cli._parse_glob('s4://dir1/dir2/')
    assert indir == 's4://dir1/dir2/'
    assert glob is None

    indir, glob = s4.cli._parse_glob('s4://dir1/dir2/*')
    assert indir == 's4://dir1/dir2/'
    assert glob == '*'

    indir, glob = s4.cli._parse_glob('s4://dir1/dir2/*/*')
    assert indir == 's4://dir1/dir2/'
    assert glob == '*/*'

    indir, glob = s4.cli._parse_glob('s4://dir1/dir2/*/*_1')
    assert indir == 's4://dir1/dir2/'
    assert glob == '*/*_1'

def test_hash():
    assert 8348297608100219689 == s4.hash('asdf')
    assert 10211175311721367273 == s4.hash('123')
    assert 7921424674728911129 == s4.hash('hello')

def test_pick_server():
    s4.servers = lambda: [('a', 123), ('b', 123), ('c', 123)]
    assert 'a:123' == s4.pick_server('s4://bucket/a.txt')
    assert 'c:123' == s4.pick_server('s4://bucket/d.txt')
    assert 'b:123' == s4.pick_server('s4://bucket/f.txt')
