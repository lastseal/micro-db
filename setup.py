from setuptools import setup

setup(
    name="micro-db",
    version="1.0.0",
    description="Wrapper de conexión con base de datos",
    author="Rodrigo Arriaza",
    author_email="hello@lastseal.com",
    url="https://www.lastseal.com",
    packages=['micro'],
    install_requires=[ 
        i.strip() for i in open("requirements.txt").readlines() 
    ]
)