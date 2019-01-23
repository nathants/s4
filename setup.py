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
                      'mmh3 == 2.5.1'],
    entry_points={'console_scripts': ['s4-server = s4.server:main',
                                      's4-cli = s4.cli:main']},
)
