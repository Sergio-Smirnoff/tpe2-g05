#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

if [[ ! $JAVA_RUN_CLASS ]]
then
    echo "Missing JAVA_RUN_CLASS variable"
    exit 1
fi

#JAVA_OPTS="-Djava.rmi.server.codebase=file://$PATH_TO_CODE_BASE/lib/jars/rmi-params-client-1.0-SNAPSHOT.jar"

MAIN_CLASS="ar.edu.itba.pod.hazelcast.client.$JAVA_RUN_CLASS"

java $JAVA_OPTS -cp 'lib/jars/*'  $MAIN_CLASS $*
