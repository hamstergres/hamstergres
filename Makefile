.PHONY: up down logs ps test test-unit test-e2e test-sysbench pgbench-prepare pgbench-run pgbench-cleanup sysbench-prepare sysbench-run sysbench-cleanup benchmark-sysbench experiment-sharding-cpu test-observability run-proxy proxy-status nest-status clean-nest-tests

BENCHMARK_MODE ?= sharded
PGBENCH_SCALE ?= 1
PGBENCH_OPTIONS ?= --select-only --client=4 --jobs=4 --time=15
SYSBENCH_OPTIONS ?= --tables=2 --table-size=1000 --threads=4 --time=15 --events=0

up:
	docker compose up -d --wait

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	go test ./...

test-unit:
	go test ./cmd/... ./internal/...

test-e2e:
	go test ./integration -count=1 -v

test-sysbench:
	HAMSTERGRES_SYSBENCH_E2E=1 go test ./integration -run '^TestSysbenchReadWriteEndToEnd$$' -count=1 -v

pgbench-prepare:
	./scripts/pgbench-workload.sh $(BENCHMARK_MODE) prepare --scale=$(PGBENCH_SCALE)

pgbench-run:
	./scripts/pgbench-workload.sh $(BENCHMARK_MODE) run $(PGBENCH_OPTIONS)

pgbench-cleanup:
	./scripts/pgbench-workload.sh $(BENCHMARK_MODE) cleanup

sysbench-prepare:
	./scripts/sysbench-workload.sh $(BENCHMARK_MODE) prepare $(SYSBENCH_OPTIONS)

sysbench-run:
	./scripts/sysbench-workload.sh $(BENCHMARK_MODE) run $(SYSBENCH_OPTIONS)

sysbench-cleanup:
	./scripts/sysbench-workload.sh $(BENCHMARK_MODE) cleanup $(SYSBENCH_OPTIONS)

benchmark-sysbench:
	HAMSTERGRES_BENCHMARK_MODE=$(BENCHMARK_MODE) ./scripts/benchmark-sysbench.sh

experiment-sharding-cpu:
	./scripts/experiment-sharding-cpu.sh

test-observability:
	./scripts/observability-smoke.sh

run-proxy:
	go run ./cmd/hamstergres-proxy --config config/hamstergres.local.example.yaml

proxy-status:
	go run ./cmd/hamstergres-proxy status

nest-status:
	./scripts/nest-status.sh

clean-nest-tests:
	./scripts/clean-nest-tests.sh
