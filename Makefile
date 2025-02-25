SHELL := /bin/bash

NUM_NODES := 5
SLEEP := 1
PARSER := docker-compose-parser.sh

.PHONY: nodes
nodes:
	@echo "Building the project..."
	@cargo build --bin cassandra_node
	@echo "Starting nodes..."
	@for i in $(shell seq 0 $(NUM_NODES)); do \
		cargo run --bin cassandra_node $$i localhost & \
		echo -e "\nNode $$i started"; \
		sleep $(SLEEP); \
	done
	@wait
	@echo -e "\nAll nodes are up and running."

.PHONY: clean
clean:
	@echo "Cleaning nodes..."
	@sudo pkill -f cassandra_node
	@sudo pkill -f cassandra

.PHONY: kill
kill:
	@echo "Killing processes listening on ports 5000$(i) and 6000$(i)..."
	@sudo lsof -ti tcp:5000$(i) -ti tcp:6000$(i) -sTCP:LISTEN | uniq | while read -r pid; do \
		echo "Killing PID: $$pid"; \
		sudo kill -9 $$pid; \
	done

.PHONY: client
client:
	@echo "Starting flights simulator and control..."
	@cargo run --bin rustic_airlines

.PHONY: ui
ui:
	@echo "Starting UI..."
	@cargo run --bin rustic_airlines ui

.PHONY: sim
sim:
	@echo "Starting flights simulator..."
	@cargo run --bin rustic_airlines sim

.PHONY: node
node:
	@echo "Running node $i..."
	@cargo run --bin cassandra_node $i localhost

.PHONY: test
test:
	@cargo test

.PHONY: docker
docker:
	@./$(PARSER)
	@docker compose up -d --build --no-recreate

.PHONY: docker-i
docker-i:
	@./$(PARSER)
	@docker compose up node$i -d --build --no-recreate

.PHONY: docker-0to4
docker-0to4:
	@./$(PARSER)
	@docker compose up node0 node1 node2 node3 node4 -d --build --no-recreate

.PHONY: down
down:
	@docker compose down