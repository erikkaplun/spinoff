from setuptools import setup, find_packages

setup(
    name="unnamedframework",
    version="0.3dev",
    packages=find_packages(),

    install_requires=[
        'twisted>=12.0',
        'txcoroutine',
        'txzmq',
        ],

    entry_points="""
    [console_scripts]
    runactor = unnamedframework.runactor:main
    """,

    author="Erik Allik",
    author_email="erik.allik@skype.net",
)
