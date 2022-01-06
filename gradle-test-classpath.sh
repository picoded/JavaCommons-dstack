#!/bin/sh

if [ -z "$1" ]
then
    echo "$0 [picoded.dstack.*]"
    exit 1
fi

./gradlew test -Ptest_mysql -Ptest_sqlite -Ptest_mssql -Ptest_oracle -Ptest_postgres -Ptest_perf --tests $1