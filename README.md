# Apache Airflow DAG: Captura de Datos hacia Cloud Storage y BigQuery (con Docker Compose)

Este proyecto muestra cómo construir un pipeline ETL simple pero completo utilizando **Apache Airflow** desplegado localmente con **Docker Compose**.

### ✅ ¿Qué hace este DAG?

- 📥 **Captura datos de usuarios aleatorios** desde una API pública (`randomuser.me`).
- 🧹 **Elimina duplicados** por correo electrónico antes de cargarlos.
- 🗃️ **Guarda los datos en archivos CSV únicos** (con timestamp) en el sistema de archivos local.
- ☁️ **Carga automáticamente estos CSV a Google Cloud Storage**, manteniendo un historial de ejecuciones.
- 📊 **Inserta los datos en una tabla de BigQuery**.
- 🔁 **Realiza una deduplicación en BigQuery** para mantener solo el último registro por correo electrónico.

### 🚀 Tecnologías utilizadas

- Apache Airflow (v2+)
- Docker Compose
- Google Cloud Storage
- BigQuery
- Python, Pandas
- API pública: https://randomuser.me/api/

### 🧠 Ideal para:

- Practicar flujos ETL reales en entornos locales.
- Entender la integración entre Airflow y servicios de Google Cloud.
- Desarrollar y testear pipelines de forma segura antes de mover a producción.

---

## Requisitos previos

### Herramientas necesarias:

- **WSL2** (si estás usando Windows)  
  [Ver video](https://www.youtube.com/watch?v=nkwvDatrKGM&ab_channel=JashTechTV)

- **Docker Desktop**  
  [Ver video](https://www.youtube.com/watch?v=jiJFDwmWrWk&ab_channel=UskoKruM2010)

- **Visual Studio Code + Python** o cualquier editor de código  
  [Ver video](https://www.youtube.com/watch?v=1E44n9NL2gw&ab_channel=OssabaTech)

- **Git** (para clonar el repositorio)  
  [Ver video](https://www.youtube.com/watch?v=wVKyeLs0hfg&ab_channel=FerDaniele)

---

## Pasos para levantar el entorno

### Paso 1: Clonar el repositorio

```bash
git clone https://github.com/Echeverria29/docker-airflow-gcp.git
cd docker-airflow-gcp
```
### Puedes desactivar los Dags de ejemplo
AIRFLOW__CORE__LOAD_EXAMPLES: 'False'

### Paso 2: Crear archivo `.env`

Este archivo define el UID de tu usuario para que los contenedores puedan trabajar con permisos correctos.

**En Linux/WSL:**

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

**En otros sistemas (como Windows):**

```env
AIRFLOW_UID=50000
```

### Paso 3: Inicializar la base de datos

Antes de ejecutar los servicios, es necesario aplicar las migraciones de la base de datos de Airflow y crear el usuario administrador:

```bash
docker compose up airflow-init
```
### Dejar este parametro en False(opcional)
Variable: AIRFLOW__CORE__LOAD_EXAMPLES

(Archivo airflow.cfg)
load_examples = False (opcional)

### Paso 4: Ejecutar Airflow

Ahora puedes iniciar todos los servicios necesarios:

```bash
docker compose up
```
### Antes de ejecutar esperar que todos los conetenedores esten en (healthy)
STATUS Up 2 minutes (healthy)

## Acceso por defecto

- **Usuario:** `airflow`  
- **Contraseña:** `airflow`

Airflow estará disponible en: [http://localhost:8080](http://localhost:8080)

---
## Conexión

Paso 1: Abre la interfaz de Airflow
Abre tu navegador y visita:
👉 http://localhost:8080

📍 Paso 2: Agregar una nueva conexión
En el menú superior, ve a Admin → Connections.

Haz clic en el botón azul "+ Add a new record" (parte superior derecha).

📍 Paso 3: Completar los campos de la conexión
Rellena el formulario con esta información:

Campo	Valor
Conn Id	google_cloud_default
Conn Type	Google Cloud
Keyfile JSON	Pega aquí el contenido completo del archivo JSON de tu service account

💡 Nota: No subas el archivo .json, simplemente abre el archivo con un editor de texto, copia todo su contenido y pégalo en el campo "Keyfile JSON".

📍 Paso 4: Guardar la conexión
Haz clic en Save.

Si todo está correcto, Airflow ya puede interactuar con BigQuery y Cloud Storage usando esa conexión.


## Solución a errores de permisos en WSL/Linux

En algunos casos, los archivos o carpetas pueden haber sido creados por el usuario `root`, lo cual impide que puedas editarlos. Aquí te mostramos cómo corregirlo.

### ✅ Cambiar el propietario del archivo o carpeta

Para cambiar un solo archivo:

```bash
sudo chown tu-usuario:tu-usuario /home/tu-usuario/airflow-docker-compose/dags/basic_dag.py
```

Para cambiar todo el directorio del proyecto:

```bash
sudo chown -R tu-usuario:tu-usuario /home/tu-usuario/airflow-docker-compose
```

### ✅ Asegurar permisos de escritura

Para toda la carpeta:

```bash
chmod -R u+rw /home/tu-usuario/airflow-docker-compose
```
---

## Limpieza del entorno

Si necesitas eliminar todo y comenzar desde cero:

```bash
docker compose down --volumes --remove-orphans
rm -rf <directorio-del-proyecto>
```

---

