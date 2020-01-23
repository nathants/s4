import setuptools
import os
import sys

setuptools.setup(
    version="0.0.1",
    license='mit',
    name='s4',
    author='nathan todd-stone',
    author_email='me@nathants.com',
    url='http://github.com/nathants/s4',
    description='stupid simple storage service',
    python_requires='>=3.6',
    install_requires=['argh >0.26, <0.27',
                      'requests >2, <3'],
    packages=['s4'],
)

parent = os.path.dirname(os.path.abspath(__file__))
scripts = [os.path.abspath(os.path.join(service, script))
           for service in os.listdir(parent)
           if service.startswith('s4')
           and os.path.isdir(service)
           for script in os.listdir(os.path.join(parent, service))
           for path in [os.path.join(service, script)]
           if os.path.isfile(path)
           and path.endswith('.py')
           and not os.path.basename(path).startswith('_')]

dst_path = os.path.dirname(os.path.abspath(sys.executable))
for src in scripts:
    name = os.path.basename(src)
    name = 's4-' + name.split('.py')[0]
    dst = os.path.join(dst_path, name)
    try:
        os.remove(dst)
    except FileNotFoundError:
        pass
    os.symlink(src, dst)
    os.chmod(dst, 0o775)
    print('link:', dst, '=>', src, file=sys.stderr)
