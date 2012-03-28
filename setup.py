from setuptools import setup, find_packages

setup(
    name="spinoff",
    version="0.1",
    packages=find_packages(),

    install_requires=[
        'twisted==12.0',
        'zope.interface',
        ],

    author="Erik Allik",
    author_email="erik.allik@skype.net",
)
