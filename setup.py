from setuptools import setup, find_packages

with open('README.rst') as file:
    long_description = file.read()

setup(
    name="spinoff",
    description="Framework for writing distributed, fault tolerant and scalable applications",
    long_description=long_description,
    version="0.4dev",
    packages=find_packages(),

    install_requires=[
        'twisted>=12.0',
        'txcoroutine',
        'txzmq==0.5.1',
    ],

    dependency_links=[
        'https://github.com/eallik/txZMQ/tarball/master#egg=txzmq-0.5.1'
    ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
    license="BSD",
    url="http://github.com/eallik/spinoff/"
)
