# Auditoria de github-fs

Fecha: 2026-05-21

## Alcance revisado

La auditoria contrasta lo que promete [`README.md`](./README.md:1) con la implementacion real en:

- [`github_fs/service.py`](./github_fs/service.py:53)
- [`github_fs/github_api.py`](./github_fs/github_api.py:37)
- [`github_fs/web.py`](./github_fs/web.py:119)
- [`github_fs/runtime.py`](./github_fs/runtime.py:20)
- [`github_fs/config.py`](./github_fs/config.py:164)
- [`gunicorn.conf.py`](./gunicorn.conf.py:27)
- tests en [`tests/test_service.py`](./tests/test_service.py:118) y modulos auxiliares

## Resumen ejecutivo

La aplicacion implementa la mayor parte del flujo base descrito en el README: deteccion por hash, cifrado AES-256-GCM, chunking, seleccion de cuenta/repositorio, persistencia en `/state/index.json`, web minima con PIN y ejecucion del scheduler dentro del master de Gunicorn.

La brecha principal esta en la verificacion. El README promete "verificar periodicamente la integridad reconstruyendo cada version desde GitHub" ([README.md](./README.md:3)), pero la implementacion solo verifica la version activa de archivos actualmente presentes en disco y la compara contra el archivo local actual, no contra la huella almacenada de la version. Eso deja fuera versiones historicas, archivos borrados localmente y genera falsos negativos si el fichero local cambia antes del siguiente sync.

Tambien hay desviaciones operativas menores pero relevantes: falta `.env.example` aunque el README lo exige, los puertos documentados no son consistentes entre README y despliegue Docker, y la suite de tests descrita en el README no puede ejecutarse en este entorno porque `pytest` no esta instalado ni declarado en `requirements.txt`.

## Cumplimiento del README

### Funcionalidades implementadas correctamente

- Deteccion de cambios por hash local: [`github_fs/service.py`](./github_fs/service.py:190)
- Cifrado AES-256-GCM y chunking: [`github_fs/crypto.py`](./github_fs/crypto.py:12), [`github_fs/service.py`](./github_fs/service.py:384)
- Seleccion de cuenta con cuota diaria disponible: [`github_fs/service.py`](./github_fs/service.py:494)
- Seleccion aleatoria de repo gestionado o creacion automatica de uno nuevo: [`github_fs/service.py`](./github_fs/service.py:518), [`github_fs/service.py`](./github_fs/service.py:574)
- Persistencia de versiones, cuentas, repos y tareas en estado: [`github_fs/state.py`](./github_fs/state.py:73), [`github_fs/service.py`](./github_fs/service.py:433)
- Web minima con PIN y acciones manuales: [`github_fs/web.py`](./github_fs/web.py:140)
- Scheduler en el proceso master de Gunicorn: [`gunicorn.conf.py`](./gunicorn.conf.py:27)

### Parcialmente implementado o con desviaciones

- Verificacion remota: existe, pero no cubre "cada version" ni es independiente del estado local actual: [`github_fs/service.py`](./github_fs/service.py:302)
- Documentacion Docker Compose: el README indica `cp .env.example .env`, pero el archivo `.env.example` no existe en el repo: [`README.md`](./README.md:82)
- Puertos: el README declara `APP_WEB_PORT=8080` como valor relevante, pero Docker y produccion usan `8083` por defecto: [`README.md`](./README.md:43), [`README.md`](./README.md:140), [`docker-compose.yml`](./docker-compose.yml:6), [`Dockerfile`](./Dockerfile:11)

## Hallazgos de auditoria

### 1. La verificacion no cumple la promesa de "cada version"

Severidad: alta

El README promete verificar la integridad reconstruyendo cada version desde GitHub ([README.md](./README.md:3), [README.md](./README.md:12)). Sin embargo, `_verify_impl()` solo:

- recorre entradas de archivo, no versiones individuales
- ignora entradas con `present=False`
- toma solo `active_version_id`

Referencias: [`github_fs/service.py`](./github_fs/service.py:308), [`github_fs/service.py`](./github_fs/service.py:311)

Impacto:

- las versiones historicas nunca se validan
- un archivo borrado localmente deja de ser verificable aunque su backup remoto siga existiendo
- la cobertura real de integridad es menor de la descrita

### 2. La verificacion compara contra el archivo local actual, no contra la huella registrada de la version

Severidad: alta

La version subida guarda `plaintext_sha256` y `source_sha256` en el manifiesto/estado ([`github_fs/service.py`](./github_fs/service.py:439), [`github_fs/service.py`](./github_fs/service.py:443)), pero la verificacion usa `sha256_file(local_path)` y falla si no coincide con el remoto reconstruido ([`github_fs/service.py`](./github_fs/service.py:322), [`github_fs/service.py`](./github_fs/service.py:338)).

Impacto:

- si el archivo local cambia entre un `sync` y el siguiente, `verify` reporta error aunque el backup remoto este perfecto
- la verificacion deja de medir integridad del almacenamiento remoto y pasa a mezclarla con drift local
- no se puede verificar una version antigua contra su propio hash historico

### 3. El README exige `.env.example`, pero el repositorio no lo incluye

Severidad: media

La guia de arranque indica `cp .env.example .env` ([`README.md`](./README.md:82)), pero el archivo no existe en el repo.

Impacto:

- el onboarding documentado no funciona tal como esta escrito
- aumenta la probabilidad de configuraciones incompletas o inconsistentes

### 4. La historia de puertos es inconsistente entre documentacion y despliegue real

Severidad: media

El README enumera `APP_WEB_PORT=8080` como valor relevante ([`README.md`](./README.md:43)), pero la receta de produccion usa `8083` ([`README.md`](./README.md:140)) y Docker Compose tambien cae en `8083` por defecto ([`docker-compose.yml`](./docker-compose.yml:6)). El `Dockerfile` expone igualmente `8083` ([`Dockerfile`](./Dockerfile:11)).

Impacto:

- el usuario no sabe con certeza cual es el puerto esperado por defecto
- puede haber despliegues mal documentados o healthchecks mal configurados

### 5. La suite anunciada en el README no es ejecutable con las dependencias declaradas

Severidad: media

El README indica ejecutar `python3 -m pytest` ([`README.md`](./README.md:143)), pero en este entorno el comando falla porque `pytest` no esta instalado. Ademas, `requirements.txt` solo contiene Flask, gunicorn, cryptography y requests ([`requirements.txt`](./requirements.txt:1)).

Resultado observado:

```text
/usr/bin/python3: No module named pytest
```

Impacto:

- la via de validacion documentada no funciona "out of the box"
- la calidad automatizada depende de una dependencia no declarada

### 6. Parte del cliente GitHub evita la capa comun de reintentos

Severidad: media

La clase `GitHubClient` define `_request()` con reintentos y backoff ([`github_fs/github_api.py`](./github_fs/github_api.py:52)), pero `ensure_branch_initialized()`, `branch_info()` y parte de `update_ref()` usan `requests.get/put` directos ([`github_fs/github_api.py`](./github_fs/github_api.py:149), [`github_fs/github_api.py`](./github_fs/github_api.py:185), [`github_fs/github_api.py`](./github_fs/github_api.py:214)).

Impacto:

- comportamiento inconsistente ante fallos transitorios
- timeouts y reintentos configurables no se aplican de forma uniforme

## Cobertura de pruebas

Existe una base razonable de tests unitarios para:

- configuracion: [`tests/test_config.py`](./tests/test_config.py:29)
- estado: [`tests/test_state.py`](./tests/test_state.py:6)
- cifrado: [`tests/test_crypto.py`](./tests/test_crypto.py:4)
- servicio y web basicos: [`tests/test_service.py`](./tests/test_service.py:118)

Pero faltan pruebas exactamente en las zonas de mayor riesgo funcional:

- verificacion de versiones historicas
- verificacion de archivos ausentes localmente
- drift local entre `sync` y `verify`
- resiliencia de llamadas GitHub con errores parciales
- coherencia de despliegue/documentacion

## Tareas pendientes dentro de lo que define el README

Si tomamos el README como contrato funcional, todavia quedan tareas por cubrir:

1. Verificar todas las versiones, no solo la activa.
2. Verificar integridad contra el hash almacenado de cada version, no contra el archivo local actual.
3. Permitir verificar versiones aunque el archivo ya no exista en `/datos`.
4. Añadir `.env.example` real y alineado con la configuracion soportada.
5. Unificar la documentacion de puertos y defaults entre README, Dockerfile y Compose.
6. Declarar dependencias de test o documentar claramente un `requirements-dev.txt` equivalente.
7. Añadir tests para los casos anteriores.

## Conclusion

El proyecto ya tiene una base funcional util y bastante cercana a lo descrito en el README en el flujo de `sync`, persistencia y panel web. La deuda principal no esta en "falta todo", sino en que la promesa de verificacion remota esta implementada de forma parcial y conceptualmente distinta a lo documentado.

Mi conclusion es que si, todavia quedan tareas importantes por cubrir dentro del alcance del README, y la prioridad numero uno deberia ser corregir el modelo de verificacion para que audite versiones remotas historicas de forma independiente del estado local.
