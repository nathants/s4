import shell
import util.iter
import shell
from shell import run
import util.retry

retry = lambda f: util.retry.retry(f, times=1000, max_seconds=60, exponent=1.5)

shell.set['stream'] = True
cluster_name = '_s4_test_cluster'
state = {}

def setup_module():
    try:
        ids = run('aws-ec2-id', cluster_name).splitlines()
    except:
        ids = run('aws-ec2-new -t i3en.large -a arch --num 3', cluster_name).splitlines()
    ips = run('aws-ec2-ip-private', cluster_name).splitlines()
    conf = '\n'.join(f'{ip}:8080' for ip in ips) + '\n'
    @retry
    def push():
        with shell.tempdir():
            with open('s4.conf', 'w') as f:
                f.write(conf)
            run('aws-ec2-scp -y s4.conf :.s4.conf', *ids)
        with shell.climb_git_root():
            run('aws-ec2-rsync -y . :/mnt/s4', cluster_name)
            run('aws-ec2-ssh -yc arch.sh', *ids)
        state['ids'] = ids
    push()

def teardown_module():
    run('aws-ec2-rm -y', cluster_name)

def setup_function():
    run('aws-ec2-ssh -yc "cd /mnt && rm -rf s4_data"', *state['ids'])
    run('aws-ec2-ssh -yc "killall -r pypy || true"', *state['ids'])
    run('aws-ec2-ssh --no-tty -yc "cd /mnt && (s4-server &> s4.log </dev/null &)"', *state['ids'])

def ssh(*a, ids=None):
    return run('aws-ec2-ssh -yc "cd /mnt && %s"' % ' '.join(map(str, a)), *(ids or state['ids']))

def test_basic():
    ids = state['ids']
    cmd = ''
    # ssh against [:1] because commands need only be issues from a single node
    # in the cluster, but will operate against all nodes in the cluster
    for i in range(10):
        cmd += f'echo data{i} | s4 cp - s4://bucket/dir/key{i}.txt\n'
    ssh(cmd, ids=ids[:1])
    xs = ssh('find s4_data -type f | wc -l').splitlines()
    xs = [int(x.split()[-1]) for x in xs]
    assert all(x > 0 for x in xs)
    cmd = 'rm -f key*.txt\n'
    for i in range(10):
        cmd += f's4 cp s4://bucket/dir/key{i}.txt .\n'
    ssh(cmd, ids=ids[:1])
    assert ssh("grep '.*' key*.txt", ids=ids[:1]).splitlines() == [
        'key0.txt:data0',
        'key1.txt:data1',
        'key2.txt:data2',
        'key3.txt:data3',
        'key4.txt:data4',
        'key5.txt:data5',
        'key6.txt:data6',
        'key7.txt:data7',
        'key8.txt:data8',
        'key9.txt:data9',
    ]
    cmd = 'rm -f key*.txt\n'
    cmd += 's4 cp s4://bucket/dir/ . --recursive\n'
    ssh(cmd, ids=ids[:1])
    assert sorted(ssh("cd dir && grep '.*' key*.txt", ids=ids[:1]).splitlines()) == [
        'key0.txt:data0',
        'key1.txt:data1',
        'key2.txt:data2',
        'key3.txt:data3',
        'key4.txt:data4',
        'key5.txt:data5',
        'key6.txt:data6',
        'key7.txt:data7',
        'key8.txt:data8',
        'key9.txt:data9',
    ]
    vals = sorted(ssh('cd s4_data && find -type f | grep -v xxh3').splitlines())
    vals = util.iter.groupby(vals, lambda x: x.split()[1])
    vals = {frozenset({x.split()[-1] for x in v}) for k, v in vals}
    result = {
        frozenset({'./bucket/dir/key0.txt',
                   './bucket/dir/key5.txt'}),
        frozenset({'./bucket/dir/key2.txt',
                   './bucket/dir/key3.txt'}),
        frozenset({'./bucket/dir/key1.txt',
                   './bucket/dir/key4.txt',
                   './bucket/dir/key6.txt',
                   './bucket/dir/key7.txt',
                   './bucket/dir/key8.txt',
                   './bucket/dir/key9.txt'}),
    }
    assert result == vals
