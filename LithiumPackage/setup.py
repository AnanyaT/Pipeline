from setuptools import setup, find_packages

setup(
    name='LithiumPackage',
    version='0.1',
    author='Ananya Tripathi',
    description='Lithium database creation',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'pytextrank',
        'keybert','htmldate','datefinder','CurrencyConverter','textacy','geopy',
        'lexnlp','quantulum3','country-converter','fuzzywuzzy','python-docx','spacy','pymongo',
        'PyPDF2','keybert','Tensorflow == 2.12.0','tensorflow_hub'
    ],
)