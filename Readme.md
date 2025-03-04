Flight data management system in airports, through the design and implementation of a distributed database (similar to Cassandra). This system provides support to our flight application, which includes a graphical interface to facilitate its manipulation. The project was developed as part of the Taller de Programación I course. Since the project was developed in a private repository, it is not possible to view the commits. 🦀

## How to Run Nodes

**Important:** To run the nodes, you must specify the numeric environment variable `DB_KEY` with a length of less than 20 digits; for example, by creating a `.env` file in the root of the project with the following content:


```bash
DB_KEY=82917
```
### Option 1: **Dockerized**

*Requires having Docker and docker-compose installed*

To start all nodes:
    
```bash
make docker
```

Then, for down nodes:
    
```bash
make down
```

### Option 2: On **localhost**

To start all nodes:
```bash
make nodes
```
Then, to clean up processes:
    
```bash
make clean
```

To start a specific node:

```bash
make node i=$i
```
To terminate a specific node:

```bash
make kill i=$i
```

## How to Run the Client
Once the nodes are started, the client can be run with the following commands:

To run the Flight Simulator:


```bash
make sim
```

To run the Flight Control graphical interface:
```bash
make ui
```
## How to Run Tests
To run the tests, execute the following command:


```bash
make test
```
