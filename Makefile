.PHONY: up down logs ps test test-unit test-e2e test-sysbench run-proxy proxy-status

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

run-proxy:
	go run ./cmd/hamstergres-proxy

proxy-status:
	go run ./cmd/hamstergres-proxy status
