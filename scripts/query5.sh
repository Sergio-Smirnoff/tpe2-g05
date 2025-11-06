#!/bin/bash

export JAVA_RUN_CLASS="ClientQuery5"

# Verificar que las clases estén compiladas
if [ ! -d "../client/target/classes" ]; then
    echo "Classes not found. Run 'mvn package' first."
    exit 1
fi

CLIENT_DIR="../client/target/tpe2-g05-client-2025.2Q"

chmod +x "$CLIENT_DIR/run-client.sh"
cd "$CLIENT_DIR"
./run-client.sh $*
cd -
unset JAVA_RUN_CLASS