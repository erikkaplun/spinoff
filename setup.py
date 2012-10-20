import os

from setuptools import setup, find_packages

if os.path.exists('README.rst'):
    with open('README.rst') as file:
        long_description = file.read()
else:
    long_description = ''

setup(
    name="spinoff",
    description="Framework for writing distributed, fault tolerant and scalable applications",
    long_description=long_description,
    version="0.4.1",
    packages=find_packages(),

    install_requires=[
        'twisted>=12.0',
        'txcoroutine',
        'txzmq==0.5.1',
    ],

    dependency_links=[
        'https://github.com/eallik/txZMQ/tarball/newapi#egg=txzmq-0.5.1'
    ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
    license="BSD",
    url="http://github.com/eallik/spinoff/"
)
