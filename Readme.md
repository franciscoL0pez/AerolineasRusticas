Sistema de gestión de datos de vuelos en aeropuertos, mediante el diseño e implementación de una base de datos distribuida (similar a Cassandra). Este sistema proporciona soporte a nuestra aplicación de vuelos, que incluye una interfaz gráfica para facilitar su manipulación. El proyecto fue realizado como parte de la materia Taller de Programación I. Debido a que el proyecto se desarrolló en un repositorio privado, no es posible visualizar los commits.  🦀
## Cómo correr nodos

**Importante:** Para correr los nodos se debe especificar la variable de entorno numérico `DB_KEY` de longitud menor a 20 dígitos; por ejemplo, creando un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```bash
DB_KEY=82917
```

### Opción 1: **Dockerizado**

*Requiere tener instalado Docker y docker-compose*

Para iniciar todos los nodos:
    
```bash
make docker
```

Luego, para bajar los nodos:
    
```bash
make down
```

### Opción 2: En **localhost**

Para iniciar todos los nodos:
```bash
make nodes
```

Luego, para limpiar los procesos:
    
```bash
make clean
```

Para iniciar un proceso en particular:

```bash
make node i=$i
```

Para terminar un proceso en particular:

```bash
make kill i=$i
```

## Cómo correr cliente
Una vez iniciados los nodos, se puede correr el cliente con los siguientes comandos:

Para correr el Simulador de Vuelos:

```bash
make sim
```

Para correr la interfaz gráfica de Control de Vuelos:

```bash
make ui
```

## Cómo testear
Para correr los tests, se debe ejecutar el siguiente comando:

```bash
make test
```
