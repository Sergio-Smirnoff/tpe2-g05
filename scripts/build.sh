#!/bin/bash

if ! (cd .. && mvn clean package)
then
    echo "Maven build failed. Exiting."
    exit 1
fi

if ! tar -xzvf ../server/target/tpe2-g05-server-2025.2Q-bin.tar.gz -C ../server/target/
then
    echo "Failed to extract server tarball. Exiting."
    exit 1
fi

if ! tar -xzvf ../client/target/tpe2-g05-client-2025.2Q-bin.tar.gz -C ../client/target/
then
    echo "Failed to extract client tarball. Exiting."
    exit 1
fi