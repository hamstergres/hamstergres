.PHONY: up down logs ps test test-unit test-e2e test-sysbench benchmark-sysbench experiment-sharding-cpu test-observability run-proxy proxy-status

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

benchmark-sysbench:
	./scripts/benchmark-sysbench.sh

experiment-sharding-cpu:
	./scripts/experiment-sharding-cpu.sh

test-observability:
	./scripts/observability-smoke.sh

run-proxy:
	go run ./cmd/hamstergres-proxy --config config/hamstergres.local.example.yaml

proxy-status:
	go run ./cmd/hamstergres-proxy status
