import setuptools

setuptools.setup(
    name = "cde_functions",
    version = "1.0",
    author = "Darpan Desai",
    author_email = "darpan.p.desai.dev@auds.army.mil",
    py_modules = ['cde_functions'],     
    python_requires = '>=3.6'
)

from .functions import *
from setuptools import setup

setup(name='cde_functions',
        version='1.0',
        description='Custom functions created for use within databricks.',        
        author=['Corey Lesko','Darpan Desai'],
        license='',
        packages=['cde_functions'],
        python_requires='>=3.6')
