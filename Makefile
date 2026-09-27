.PHONY: up down

up:
	docker compose up --build --detach

down:
	docker compose down --volumes --remove-orphans
