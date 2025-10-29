#!/bin/bash

export JAVA_RUN_CLASS="ClientQuery2"

# Verificar que las clases estén compiladas
if [ ! -d "client/target/classes" ]; then
    echo "Classes not found. Run 'mvn package' first."
    exit 1
fi

cd client/target/tpe2-g05-client-2025.2Q
./run-client.sh $*
cd -
unset JAVA_RUN_CLASS