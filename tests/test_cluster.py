import shell
import s4.server
from shell import run

cluster_name = '_s4_test_cluster'

state = {}

def setup_module():
    state['context'] = shell.set_stream()
    state['context'].__enter__()
    try:
        ids = run('aws-ec2-id', cluster_name).splitlines()
    except:
        ids = run('aws-ec2-new -t z1d.large -a arch --num 3', cluster_name).splitlines()
    ips = run('aws-ec2-ip-private', cluster_name).splitlines()
    with shell.climb_git_root():
        run('aws-ec2-rsync -y . :s4', cluster_name)
    conf = '\n'.join(f'{ip}:8080' for ip in ips) + '\n'
    with shell.tempdir():
        with open('s4.conf', 'w') as f:
            f.write(conf)
        run('aws-ec2-scp -y s4.conf :.s4.conf', *ids)
    with shell.climb_git_root():
        run('cat arch.sh | aws-ec2-ssh -yc -', *ids)
    state['ids'] = ids

def teardown_module():
    state['context'].__exit__(None, None, None)

def setup_function():
    run(f'aws-ec2-ssh -yc "rm -rf {s4.server.path_prefix}"', *state['ids'])
    run('aws-ec2-ssh -yc "killall -r s4-server || true"', *state['ids'])
    run('aws-ec2-ssh --no-tty -yc "s4-server &> s4.log </dev/null &"', *state['ids'])

def ssh(*a, ids=None):
    return run('aws-ec2-ssh -yc "%s"' % ' '.join(map(str, a)), *(ids or state['ids']))

def test_basic():
    ids = state['ids']
    cmd = ''
    for i in range(10):
        cmd += f'echo data{i} | s4-cli cp - s4://bucket/dir/key{i}.txt\n'
    ssh(cmd, ids=ids[:1])
    xs = ssh('find', s4.server.path_prefix, '-type f | wc -l').splitlines()
    xs = [int(x.split()[-1]) for x in xs]
    assert all(x > 0 for x in xs)
    cmd = 'rm -f key*.txt\n'
    for i in range(10):
        cmd += f's4-cli cp s4://bucket/dir/key{i}.txt .\n'
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
    cmd += f's4-cli cp s4://bucket/dir/ . --recursive\n'
    ssh(cmd, ids=ids[:1])
    assert ssh("cd dir && grep '.*' key*.txt", ids=ids[:1]).splitlines() == [
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
