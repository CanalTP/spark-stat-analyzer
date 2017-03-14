#!/bin/bash

./run_migrations.sh

echo 'Run analyzer: '${TABLE}

YESTERDAY_DATE=`date --date="yesterday" +%Y-%m-%d`
START_DATE=${START_DATE:-$YESTERDAY_DATE}
END_DATE=${END_DATE:-$YESTERDAY_DATE}
PROGRESS_BAR=${PROGRESS_BAR:-true}

${SPARK_BIN}spark-submit \
    --py-files spark-stat-analyzer.zip \
    --conf spark.ui.showConsoleProgress=$PROGRESS_BAR \
    --master $MASTER_OPTION \
    --driver-java-options '-Duser.timezone=UTC' \
    manage.py \
    -a ${TABLE} \
    -i $CONTAINER_STORE_PATH \
    -s $START_DATE \
    -e $END_DATE

RESULT=$?

if [ $RESULT -eq 0 ]
then
    echo "Analyzer done"
else
    echo "Analyzer failed"
fi

exit $RESULT
