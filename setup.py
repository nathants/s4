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
                      'https://github.com/nathants/py-web/tarball/0689408bd64ea9e924abda9fdfd0053b0c2907d2#egg=py-web-0.0.1',
                      'https://github.com/nathants/py-schema/tarball/ce359a91d2f7156d60e6ceb73f955af4ed333ee8#egg=py-schema-0.0.1'],
    entry_points={'console_scripts': ['s4-server = s4.server:main',
                                      's4-cli = s4.cli:main']},
)
