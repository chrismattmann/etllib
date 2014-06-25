#!/usr/bin/env python
# encoding: utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
# $Id$

import ez_setup, os.path
ez_setup.use_setuptools()
from setuptools import setup, find_packages

version = '0.0.0'

_descr = u'''**********
kivatikasolrlib
***************

.. contents::

KivaTikaSolrlib provides functionality for munging through and repackaging
Kiva JSON data for preparation and submission (ETL) to Apache Solr. The 
library takes advantage of Apache Tika, and is callable from Apache OODT.

'''
_keywords = 'xdata kiva tika solr oodt jpl mda isi'
_classifiers = [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database :: Front-Ends',
    'Topic :: Scientific/Engineering',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

long_description = _descr + read('docs', 'INSTALL.txt') + '\n' + read('docs', 'USE.txt') + '\n' + read('docs', 'HISTORY.txt') + '\n'
open('doc.txt', 'w').write(long_description)

setup(
    name='kivatikasolrlib',
    version=version,
    description='Kiva Tika Solr lib',
    long_description=long_description,
    classifiers=_classifiers,
    keywords=_keywords,
    author='Chris Mattmann',
    author_email='chris.a.mttmnn@nasa.gov',
    url='http://xdata-public.jpl.nasa.gov/search/',
    download_url='http://xdata-public.jpl.nasa.gov/dist/kiva',
    license=read('docs', 'LICENSE.txt'),
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=['kiva'],
    include_package_data=True,
    zip_safe=True,
    test_suite='kiva.tests',
    entry_points={
        'console_scripts': [
            'kivaposter = kiva.poster:main',
            'kivarepackage = kiva.repackage:main',
            'kivarepackageandpost = kiva.repackageandpost:main',
        ],
    }, 
    package_data = {
        # And include any *.conf files found in the 'conf' subdirectory
        # for the package
    },
    install_requires=[
        'setuptools',
        'iso8601',
        'jcc',
        'tika>=1.4',
    ],
    dependency_links=[
        'https://github.com/chrismattmann/python-tika-with-deps/zipball/master#egg=tika-1.4'
    ],
)
