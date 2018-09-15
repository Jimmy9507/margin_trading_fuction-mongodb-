#!/usr/bin/env bash


cd ..

#pip install -U --no-deps .
#pip install .
#python setup.py compile_catalog
#pip install -U --no-deps .
#pip install .

FULLNAME=`python3.4 setup.py --fullname`

python3.4 setup.py bdist_wheel
sshpass -p rice rsync ./dist/$FULLNAME*  rice@pypi.ricequant.com:/srv/pypi/wheelhouse/  -razP --checksum
cd -

