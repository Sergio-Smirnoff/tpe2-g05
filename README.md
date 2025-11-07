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
3. Ejecuta el script `build.sh` para compilar el código fuente. Este script también se encargará de descomprimir automáticamente los archivos `tar` necesarios.

```bash
  ./build.sh
```

## Ejecución del Servidor

Se ejecutará el servidor con la cantidad de nodos seleccionados.
(Si no se aclara se ejecutara con uno solo)

1. Ejecutar el servidor con el script `server.sh` 

```bash
  ./server.sh -n X -Dinterface=yy.yy.yy.yy 
```

| Parametro       | Descripcion                                                    | Default   |
|:----------------|:---------------------------------------------------------------|:----------|
| **-n**          | Cantidad de instancias del servidor que se desean ejecutar     | 1         |
| **-Dinterface** | Interfaz de red para el 'bind' del servidor y armar el cluster | 127.0.0.1 | 

Ejemplo de Uso:

```bash
  ./server.sh -n 2 -Dinterface='192.168.1.*'  
```

## Ejecucion de consultas

Para ejecutar cualquiera de las queries, se utiliza el script principal con la siguiente sintaxis.

Sintaxis General:

```bash
  sh queryX.sh -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [-Dborough=ZZ]
```

| Parametro       | Descripcion                                                                                                 | 
|:----------------|:------------------------------------------------------------------------------------------------------------|
| **queryX.sh**   | Script de shell especifico para la Query X.                                                                 | 
| **-Daddresses** | Direcciones IP y puertos de los nodos del cluster. Multiples nodos deben ir separados por punto y coma (;). | 
| **-DinPath**    | Ruta del directorio que contiene los archivos de datos de entrada (viajes y zonas).                         | 
| **-DoutPath**   | Ruta del directorio donde se generaran los archivos de salida: queryX.csv y timeX.txt.                      | 
| **-Dborough**   | Parametro opcional para query4 (Nombre de la zona sobre el cual se ejecutará la query).                     | 

Ejemplo de Uso:

```bash
  sh query1.sh -Daddresses='10.6.0.1:5701;10.6.0.2:5701' -DinPath=/afs/it.itba.edu.ar/pub/pod/ -DoutPath=/afs/it.itba.edu.ar/pub/pod-write/
```
