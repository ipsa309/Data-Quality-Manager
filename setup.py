from gettext import install
from setuptools import setup
setup(name="DQM",
version="3.4",
description="Data Quality framework",
author="Santosh Birje, Ankur Abhishek, Mohammad Hamza, Ipsa Panda",
author_email="",
packages=['DQM'],
install_requires=['pandas==1.2.4','numpy','datetime','cryptohash==1.0.5','openpyxl','xlrd','htmlmin'])