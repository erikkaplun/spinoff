from setuptools import setup, find_packages

setup(
    name="spinoff",
    version="0.3dev",
    packages=find_packages(),

    data_files=[('twisted/plugins', ['twisted/plugins/startnode.py'])],

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
)
