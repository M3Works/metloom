#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'geopandas>=1.0.0,<2.0.0',
    'pandas>=1.0.0,<3.0.0',
    'lxml>=5.4.0,<6.0.0',
    'requests>2.0.0,<3.0.0',
    'beautifulsoup4>4,<5',
    'zeep>4.0.0',
    'pydash>=8.0.0,<9.0.0',
]

test_requirements = ['pytest>=3', ]

setup(
    author="M3Works",
    author_email='m3worksllc@gmail.com',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    description="Location Oriented Observed Meteorology (LOOM)",
    entry_points={
        'console_scripts': [
            'metloom=metloom.cli:main'
        ],
    },
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='metloom',
    name='metloom',
    packages=find_packages(include=['metloom', 'metloom.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/M3Works/metloom',
    version='0.8.0',
    zip_safe=False,
)
