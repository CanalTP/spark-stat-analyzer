#!/bin/bash

python -m pytest -v --junitxml junit.xml
RESULT=$?
find -name "*.pyc" -delete
rm -rf spark-warehouse
rm -rf tests/__pycache__
exit $RESULT
