import os

from setuptools import setup, find_packages

if os.path.exists('README.rst'):
    with open('README.rst') as file:
        long_description = file.read()
else:
    long_description = ''

setup(
    name="spinoff",
    description="Framework for writing distributed, fault tolerant and scalable internet applications",
    long_description=long_description,
    version="0.7.18",
    packages=find_packages(),

    install_requires=[
        'zope.interface',
        'pyzmq==13.1',
        'gevent==23.9.1',
    ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
    license="BSD",
    url="http://github.com/eallik/spinoff/",

    entry_points={
        'console_scripts': [
            'spin = spinoff.actor.spin:console',
        ]
    },

)
