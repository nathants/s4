import setuptools


setuptools.setup(
    version="0.0.1",
    license='mit',
    name='s4',
    author='nathan todd-stone',
    author_email='me@nathants.com',
    url='http://github.com/nathants/s4',
    description='stupid simple storage service',
    python_requires='>=3.6',
    packages=['s4'],
    install_requires=['argh >0.26, <0.27',
                      'requests >2, <3',
                      'mmh3 == 2.5.1',
                      'py-util',
                      'py-pool',
                      'py-shell',
                      'py-web',
                      'py-schema'],
    dependency_links=['https://github.com/nathants/py-util/tarball/0a45ac7fca5c3b4ccf33f019a5459cc5c5ab467a#egg=py-util-0.0.1',
                      'https://github.com/nathants/py-pool/tarball/51bddeb322a3abb2c493a3d3d3d0136590e49068#egg=py-pool-0.0.1',
                      'https://github.com/nathants/py-shell/tarball/58cd56662aa349837227ea5b5c6b3f0a857903e4#egg=py-shell-0.0.1',
                      'https://github.com/nathants/py-web/tarball/3ba52dc00d2d4200242028a9f5928f378867e6e2#egg=py-web-0.0.1',
                      'https://github.com/nathants/py-schema/tarball/4ca9827e06c5422e0988ba2be1e4478f6901b69e#egg=py-schema-0.0.1'],
    entry_points={'console_scripts': ['s4-server = s4.server:main',
                                      's4-cli = s4.cli:main']},
)
