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
    version="0.7.11",
    packages=find_packages() + ['geventreactor'],

    install_requires=[
        'zope.interface',
        'pyzmq==13.1',
        'gevent==1.0rc2',
        'lockfile==0.9.1',
    ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
    license="BSD",
    url="http://github.com/eallik/spinoff/"
)
