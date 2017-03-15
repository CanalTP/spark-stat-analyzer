#!/bin/bash

pushd migrations
echo 'Run migration database'
PYTHONPATH=../ alembic upgrade head
popd

RESULT=$?
if [ $RESULT -eq 0 ]
then
    echo "Migration database done"
    exit 0
else
    echo "Migration database failed"
    exit 1
fi

