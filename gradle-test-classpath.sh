#!/bin/sh

if [ -z "$1" ]
then
    echo "$0 [picoded.dstack.*]"
    exit 1
fi

./gradlew test --tests $1