.PHONY: setup up migrate seed test test-unit test-integration test-e2e load-test validate down logs
setup:
	@test -f .env || cp .env.example .env
	cd web && npm ci
up:
	docker compose up --build -d
migrate:
	@echo "Migrations are applied by PostgreSQL on first volume initialization."
seed:
	@echo "Synthetic responders and resources are seeded by the application at startup."
test: test-unit
	cd web && npm test -- --passWithNoTests
test-unit:
	go test -race ./...
test-integration:
	go test -race -tags=integration ./tests/integration/...
test-e2e:
	go test -race -tags=e2e ./tests/end-to-end/...
load-test:
	k6 run tests/load/incidents.js
validate:
	go vet ./...
	cd web && npm run build
	kubectl kustomize infra/kubernetes >/dev/null
down:
	docker compose down
logs:
	docker compose logs -f app
