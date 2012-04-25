from setuptools import setup, find_packages

setup(
    name="unnamedframework",
    version="0.1.6",
    packages=find_packages(),

    install_requires=[
        'twisted==12.0',
        'zope.interface',
        'txzmq',
        ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
)
