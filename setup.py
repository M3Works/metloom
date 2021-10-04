#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

test_requirements = ['pytest>=3', ]

setup(
    author="M3Works",
    author_email='m3worksllc@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Location Oriented Observed Meteorology",
    entry_points={
        'console_scripts': [
            'dataloom=dataloom.cli:main',
        ],
    },
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='dataloom',
    name='dataloom',
    packages=find_packages(include=['dataloom', 'dataloom.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/micah-prime/dataloom',
    version='0.1.0',
    zip_safe=False,
)
