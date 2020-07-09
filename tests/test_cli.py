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
