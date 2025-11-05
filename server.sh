#!/bin/bash

export JAVA_RUN_CLASS="Server"
export JAVA_OPTS="-Xms3G -Xmx4G"

# Verificar que las clases estén compiladas
if [ ! -d "client/target/classes" ]; then
    echo "Classes not found. Run 'mvn package' first."
    exit 1
fi

cd server/target/tpe2-g05-server-2025.2Q
for (( i=0; i<2; i++ ))
do
  echo "--- Starting server (run $((i+1))) ---"
  . ./run-server.sh &
done
wait
cd -
unset JAVA_RUN_CLASS
unset JAVA_OPTS