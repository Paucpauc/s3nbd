# s3nbd

[English](README.md) | [Русский](README_RU.md) | [简体中文](README_CN.md) | Español | [Français](README_FR.md)

Servidor NBD escrito en Go que almacena dispositivos de bloques en un almacenamiento compatible con S3. Admite varios dispositivos con nombre, con un cliente activo por dispositivo. NBD utiliza TCP en la interfaz de loopback, sin TLS; S3 utiliza HTTP o HTTPS, direccionamiento path-style y Signature V4.

## Inicio rápido

Necesitas Go 1.26+, un bucket S3 existente y `nbd-client` en Linux.

```sh
go build -o s3nbd .
cp config.example.json config.json
```

Edita `config.json` para indicar el endpoint, la región, el bucket y los tamaños de los dispositivos; después inicia el servidor:

```sh
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
./s3nbd -config config.json
```

En otra terminal, conecta los dispositivos:

```sh
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N disk1
sudo nbd-client 127.0.0.1 10809 /dev/nbd1 -N disk2
```

Ahora puedes formatearlos y montarlos con las herramientas habituales de Linux. Antes de detener el servidor, desmonta los sistemas de archivos y desconecta NBD.

## Configuración

Consulta [config.example.json](config.example.json) para ver un ejemplo completo.

| Parámetro | Función |
| --- | --- |
| `LogLevel` | Nivel mínimo de registro: `debug`, `info` (predeterminado), `warn` o `error`; no distingue mayúsculas de minúsculas. Se aplica al iniciar; los valores no válidos impiden el arranque. Los registros se envían a stderr con marcas de tiempo y niveles. Las estadísticas y los eventos del ciclo de vida usan info; las conexiones y los comandos NBD, debug; las advertencias, warn; y los fallos de las operaciones, error. |
| `Listen` | Dirección NBD; debe ser una IP de loopback. |
| `Endpoint`, `Region` | Endpoint S3 y región utilizada para la firma. |
| `Devices` | `Name`, `Bucket`, `Prefix` y `Size` (en bytes) de cada dispositivo. |
| `BlockSize` | Bytes por objeto S3. Los tamaños de los dispositivos deben ser múltiplos de este valor. |
| `CacheBlocks` | Bloques en caché por dispositivo. |
| `Workers` | Concurrencia de S3 por dispositivo y número máximo de lecturas NBD activas. |
| `FlushSeconds` | Intervalo de volcado en segundo plano. |
| `YandexPatch` | Activa PATCH de Yandex Object Storage; desactivado de forma predeterminada. |
| `InsecureTLS` | Desactiva la verificación del certificado S3; desactivado de forma predeterminada. |

Define `S3_SESSION_TOKEN` para usar credenciales temporales. Para una CA personalizada, utiliza el almacén de confianza del sistema o `SSL_CERT_FILE`.

## Almacenamiento y durabilidad

**Las escrituras se confirman en RAM. Una caída provoca la pérdida de los datos que aún no se hayan volcado.** FLUSH y las escrituras con FUA esperan hasta que los cambios pendientes lleguen a S3. Las cargas fallidas permanecen pendientes para reintentarlas y el cliente recibe `EIO`.

Los volcados en segundo plano se ejecutan cada `FlushSeconds`; la desconexión y SIGINT/SIGTERM también provocan un volcado. Si el volcado final falla, el servidor termina con un error y no puede conservar el contenido restante de la RAM.

Cada objeto en `Prefix/blocks/<16 hex digits>` contiene un bloque. Los objetos ausentes se leen como ceros. Las escrituras se acumulan en la caché sin solicitudes GET. El volcado de un bloque parcialmente modificado carga los datos existentes antes de subir el bloque completo; una sobrescritura completa omite esa lectura. Los bloques que solo contienen ceros se eliminan en lugar de subirse.

La caché almacena hasta `CacheBlocks × BlockSize` bytes **por dispositivo**, además de la memoria de las solicitudes y del SDK. Las lecturas activas pueden utilizar otros `Workers × 32 MiB` para los búferes de respuesta. Cuando se llena, la caché expulsa un bloque sin cambios pendientes o vuelca bloques modificados. Las lecturas se ejecutan en paralelo; las escrituras, los descartes y los volcados esperan a que terminen las lecturas anteriores.

Ten en cuenta estas restricciones:

- No cambies los tamaños de los dispositivos ni `BlockSize` para datos existentes; solo se guardan en la configuración.
- Utiliza un único servidor por bucket/prefijo y no modifiques sus objetos externamente.
- El almacenamiento debe ofrecer consistencia de lectura tras escritura y durabilidad de las escrituras confirmadas. Un volcado que abarca varios objetos no es atómico.
- NBD utiliza negociación fixed-newstyle con exportaciones con nombre, READ/WRITE, FLUSH, FUA y TRIM. Las solicitudes READ/WRITE están limitadas a 32 MiB; no se anuncia compatibilidad con múltiples conexiones.

## Descarte

Ejecuta `sudo fstrim /mountpoint` para liberar los bloques del sistema de archivos que no se utilizan. Los descartes de bloques completos se convierten en eliminaciones S3 por lotes; los descartes parciales ponen a cero el rango seleccionado. Ambos se almacenan en búfer hasta el volcado, y las lecturas devuelven ceros inmediatamente. Las eliminaciones fallidas permanecen pendientes para reintentarlas.

Si el versionado de S3 está activado, la eliminación crea un marcador de eliminación. Utiliza una política de ciclo de vida del almacenamiento para eliminar las versiones antiguas.

## PATCH de Yandex Object Storage

Configura `"YandexPatch": true`, `"Region": "ru-central1"` y el endpoint HTTPS correspondiente. **El versionado del bucket debe estar desactivado.** PATCH es una extensión de Yandex, no forma parte del estándar S3.

Este modo envía únicamente los rangos modificados para las actualizaciones parciales de objetos existentes, evitando descargar y subir bloques completos. Los objetos nuevos y las sobrescrituras completas utilizan PUT; los descartes completos utilizan eliminación por lotes. Ante una respuesta PATCH `404 NoSuchKey`, se recurre a un PUT del bloque completo con ceros fuera de los rangos modificados. Los demás errores dejan el bloque pendiente de reintento y hacen que FLUSH/FUA fallen.

Consulta la [documentación de la API PATCH](https://yandex.cloud/ru/docs/storage/s3/api-ref/object/patch).

## Registros y pruebas

Cada cinco segundos, el servidor registra:

- `S3 stats`: número de solicitudes y bytes del cuerpo enviados/recibidos por método HTTP, para todos los dispositivos. Incluye reintentos y fallos; excluye las cabeceras y la sobrecarga de TCP/TLS.
- `cache stats`: objetos y bytes pendientes de volcado por dispositivo. Los bytes corresponden a bloques completos, incluidas las eliminaciones pendientes, y no solo a los rangos modificados.

Ejecuta las pruebas con:

```sh
go test -race ./...
```
