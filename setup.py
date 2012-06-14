from setuptools import setup, find_packages

setup(
    name="unnamedframework",
    version="0.2.4",
    packages=find_packages(),

    install_requires=[
        'twisted>=12.0',
        'txzmq',
        ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
)
