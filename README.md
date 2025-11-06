# TRABAJO PRACTICO ESPECIAL 2

___

## Integrantes:

- Campoli, Lucas - 63295
- Coleur, Matías - 63461
- Smirnoff, Sergio - 62256
- Schvartz Tallone, Martina - 62560

___

## Ejecución del Entorno

1. Abrir la terminal en el directorio tpe2-g05.
2. Cambiar al directorio scripts 'cd scripts'
3. Ejecutar 'sh ./build.sh' (El mismo compila el proyecto y descomprime los tar necesarios).

## Ejecución del Servidor

Se ejecutará el servidor con la cantidad de nodos seleccionados.
(Si no se aclara se ejecutara con uno solo)

1. Ejecutar el servidor con 'sh ./server.sh -n X' siendo X la cantidad de nodos.


## Query 1

Ejecutar el siguiente comando desde el directorio scripts/

"sh query1.sh -Daddresses='xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=."

Donde:
- xx.xx.xx.xx : La ip de la ubicación del servidor (Si es local colocar 127.0.0.1).
- yyyy : El puerto donde escucha el servidor.
- DinPath: Es el path relativo a la posicion del client/src/main/assembly/overlay de los archivos de entrada
- DoutPath: Es el path relativo a la posicion del client/target/tpe2-g5-client-2025.2Q/ de los archivos de salida

(Los archivos a ejecutar se deberan llamar "trips.csv" y "zones.csv")

## Query 2

Ejecutar el siguiente comando desde el directorio scripts/

"sh query2.sh -Daddresses='xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=."

Donde:
- xx.xx.xx.xx : La ip de la ubicación del servidor (Si es local colocar 127.0.0.1).
- yyyy : El puerto donde escucha el servidor.
- DinPath: Es el path relativo a la posicion del client/src/main/assembly/overlay de los archivos de entrada
- DoutPath: Es el path relativo a la posicion del client/target/tpe2-g5-client-2025.2Q/ de los archivos de salida

(Los archivos a ejecutar se deberan llamar "trips.csv" y "zones.csv")

## Query 3
Ejecutar el siguiente comando desde el directorio scripts/

"sh query3.sh -Daddresses='xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=."

Donde:
- xx.xx.xx.xx : La ip de la ubicación del servidor (Si es local colocar 127.0.0.1).
- yyyy : El puerto donde escucha el servidor.
- DinPath: Es el path relativo a la posicion del client/src/main/assembly/overlay de los archivos de entrada
- DoutPath: Es el path relativo a la posicion del client/target/tpe2-g5-client-2025.2Q/ de los archivos de salida

(Los archivos a ejecutar se deberan llamar "trips.csv" y "zones.csv")

## Query 4

Ejecutar el siguiente comando desde el directorio scripts/

"sh query4.sh -Daddresses='xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=. -Dborough= Z"

Donde:
- xx.xx.xx.xx : La ip de la ubicación del servidor (Si es local colocar 127.0.0.1).
- yyyy : El puerto donde escucha el servidor.
- DinPath: Es el path relativo a la posicion del client/src/main/assembly/overlay de los archivos de entrada
- DoutPath: Es el path relativo a la posicion del client/target/tpe2-g5-client-2025.2Q/ de los archivos de salida
- Dborough: Es el barrio de inicio de viaje

(Los archivos a ejecutar se deberan llamar "trips.csv" y "zones.csv")

## Query 5

Ejecutar el siguiente comando desde el directorio scripts/

"sh query1.sh -Daddresses='xx.xx.xx.xx:yyyy' -DinPath=. -DoutPath=."

Donde:
- xx.xx.xx.xx : La ip de la ubicación del servidor (Si es local colocar 127.0.0.1).
- yyyy : El puerto donde escucha el servidor.
- DinPath: Es el path relativo a la posicion del client/src/main/assembly/overlay de los archivos de entrada
- DoutPath: Es el path relativo a la posicion del client/target/tpe2-g5-client-2025.2Q/ de los archivos de salida

(Los archivos a ejecutar se deberan llamar "trips.csv" y "zones.csv")
