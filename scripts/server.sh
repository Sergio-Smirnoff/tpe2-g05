#!/bin/bash

export JAVA_RUN_CLASS="Server"
export JAVA_OPTS="-Xms3G -Xmx4G"

NODE_COUNT=1


while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -n)
      NODE_COUNT="$2"
      shift
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if ! [[ "$NODE_COUNT" =~ ^[0-9]+$ ]]; then
  echo "Error: El valor para -n ('$NODE_COUNT') no es un válido" >&2
  exit 1
fi

if [ ! -d "../client/target/classes" ]; then
    echo "Classes not found. Run 'mvn package' first."
    exit 1
fi

cd ../server/target/tpe2-g5-server-2025.2Q
chmod +x ./run-server.sh

for (( i=0; i<$NODE_COUNT; i++ ))
do
  echo "--- Starting server (run $((i+1))) ---"
  ./run-server.sh $* &
done

cd -
unset JAVA_RUN_CLASS
unset JAVA_OPTS
