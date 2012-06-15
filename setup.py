from setuptools import setup, find_packages

setup(
    name="spinoff",
    version="0.2.4",
    packages=find_packages(),

    install_requires=[
        'twisted>=12.0',
        'txzmq',
        ],

    entry_points="""
    [console_scripts]
    runactor = spinoff.runactor:main
    """,

    author="Erik Allik",
    author_email="erik.allik@skype.net",
)
