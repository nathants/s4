import sys
import shell
import s4
import os
import hashlib
import requests
import mmh3

# tmpdir = None

# def _hash(x):
#     return hashlib.sha1(bytes(x, 'utf-8')).hexdigest()

# def _cache_path(key):
#     return '%s/%s' % (tmpdir, _hash(key))

# def _cache_path_prefix(key):
#     return '%s/s3_stubbed_cache.%s.index' % (tmpdir, _hash(key))

# def _prefixes(key):
#     xs = key.split('/')
#     xs = xs[:-1]
#     xs = ['/'.join(xs[:i]) + '/' for i, _ in enumerate(xs, 1)]
#     return [""] + xs

# def rm(url, recursive=False):
#     url = url.split('s3://')[-1]
#     if recursive:
#         try:
#             with open(_cache_path_prefix(url)) as f:
#                 xs = f.read().splitlines()
#         except FileNotFoundError:
#             try:
#                 url = os.path.dirname(url) + '/'
#                 with open(_cache_path_prefix(url)) as f:
#                     xs = [x for x in f.read().splitlines() if x.startswith(url)]
#             except FileNotFoundError:
#                 sys.exit(0)
#         for x in set(xs):
#             rm(x)
#     else:
#         if not os.path.isfile(_cache_path(url)):
#             sys.exit(1)
#         else:
#             os.remove(_cache_path(url))
#             for prefix in _prefixes(url):
#                 with open(_cache_path_prefix(prefix)) as f:
#                     val = '\n'.join([x for x in f.read().splitlines() if x != url])
#                 with open(_cache_path_prefix(prefix), 'w') as f:
#                     f.write(val)

# def ls(url, recursive=False):
#     orig_url = url = url.split('s3://')[-1]
#     try:
#         with open(_cache_path_prefix(url)) as f:
#             xs = f.read().splitlines()
#     except FileNotFoundError:
#         try:
#             url = os.path.dirname(url) + '/'
#             with open(_cache_path_prefix(url)) as f:
#                 xs = [x for x in f.read().splitlines() if x.startswith(orig_url)]
#         except FileNotFoundError:
#             sys.exit(1)
#     if recursive:
#         xs = ['_ _ _ %s' % '/'.join(x.split('/')[1:]) for x in xs]
#     else:
#         xs = [x.split(url)[-1].lstrip('/') for x in xs]
#         xs = {'  PRE %s/' % x.split('/')[0]
#               if '/' in x else
#               '_ _ _ %s' % x
#               for x in xs}
#     return sorted(xs)

def cp(src, dst, recursive=False):
    if recursive:
        pass
        # if src.startswith('s3://'):
        #     bucket, *parts = src.split('s3://')[-1].rstrip('/').split('/')
        #     prefix = '/'.join(parts)
        #     for x in ls(src, recursive=True):
        #         key = x.split()[-1]
        #         token = os.path.dirname(prefix) if dst == '.' else prefix
        #         path = os.path.join(dst, key.split(token)[-1].lstrip(' /'))
        #         os.makedirs(os.path.dirname(path), 0o777, True)
        #         cp('s3://' + os.path.join(bucket, key), path)
        # elif dst.startswith('s3://'):
        #     for dirpath, dirs, files in os.walk(src):
        #         path = dirpath.split(src)[-1].lstrip('/')
        #         for file in files:
        #             cp(os.path.join(dirpath, file), os.path.join(dst, path, file))
    elif src.startswith('s3://') and dst.startswith('s3://'):
        pass
        # src = src.split('s3://')[1]
        # dst = dst.split('s3://')[1]
        # assert os.system('cp %s %s' % (_cache_path(src), _cache_path(dst))) == 0
        # for prefix in _prefixes(dst):
            # with open(_cache_path_prefix(prefix), 'a') as f:
                # f.write(dst + '\n')
    elif src.startswith('s3://'):
        pass
        # src = src.split('s3://')[1]
        # try:
            # with open(_cache_path(src), 'rb') as f:
                # x = f.read()
        # except FileNotFoundError:
            # sys.exit(1)
        # if dst == '-':
            # sys.stdout.buffer.write(x)
        # elif os.path.isdir(dst):
            # with open(os.path.join(dst, os.path.basename(src)), 'wb') as f:
                # f.write(x)
        # else:
            # with open(dst, 'wb') as f:
                # f.write(x)
    elif dst.startswith('s3://'):
        if src == '-':
            pass
            # x = sys.stdin.buffer.read()
        else:
            server = pick_server(dst)
            resp = requests.post(f'http://{server}:{http_port}/prepare_put?key={dst}')
            assert resp.status_code == 200, resp
            uuid, nc_port = resp.json()
            resp = shell.run('timeout 120 cat', src, '| xxhsum | nc', server, nc_port, warn=True) # TODO timeout is a bit crude, do something else?
            assert resp['exitcode'] == 0, resp
            checksum = resp['stderr']
            resp = requests.post(f'http://{server}:{http_port}/confirm_put?uuid={uuid}&checksum={checksum}')
            assert resp.status_code == 200, resp
    else:
        print('something needs s3://')
        sys.exit(1)

def pick_server(dst):
    return s4.servers[mmh3.hash(dst) % s4.num_servers]

# def clear_storage():
#     assert tmpdir and tmpdir.startswith('/tmp/')
#     print('$ rm -rf', tmpdir)
#     assert os.system('rm -rf %s' % tmpdir) == 0

def main():
    if len(sys.argv) < 2:
        print('usage: aws s3 cp|ls|rm')
        sys.exit(1)
    else:
        cmd = sys.argv[1]
        if cmd == 'cp':
            if len(sys.argv) < 4:
                print('usage: aws s3 cp SRC DST [--recursive]')
                sys.exit(1)
            else:
                cp(sys.argv[2], sys.argv[3], len(sys.argv) > 4 and sys.argv[4] == '--recursive')
        # elif cmd == 'ls':
        #     if len(sys.argv) < 3:
        #         print('usage: aws s3 ls URL [--recursive]')
        #         sys.exit(1)
        #     else:
        #         for x in ls(sys.argv[2], len(sys.argv) > 3 and sys.argv[3] == '--recursive'):
        #             print(x)
        # elif cmd == 'rm':
        #     if len(sys.argv) < 3:
        #         print('usage: aws s3 rm URL [--recursive]')
        #         sys.exit(1)
        #     else:
        #         rm(sys.argv[2], len(sys.argv) > 3 and sys.argv[3] == '--recursive')
        else:
            sys.exit(1)
