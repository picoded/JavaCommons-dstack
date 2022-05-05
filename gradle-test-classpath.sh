#!/bin/sh

if [ -z "$1" ]
then
    echo "$0 [picoded.dstack.*]"
    exit 1
fi

./gradlew test -Ptest_all -Ptest_perf --tests $1