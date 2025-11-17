##Levantar proyecto con docker-compose
run-project:
	docker compose up -d

##Ver logs del servicio principal
logs:
	docker compose logs

##Detener contenedores
stop:
	docker compose down

##Ejecutar tests con pytest desde Python
test:
	pytest -vv

##Ayuda
help:
	@echo "Comandos disponibles:"
	@echo "  make run-project   -> Levantar proyecto con Docker Compose"
	@echo "  make logs          -> Ver logs del servicio principal"
	@echo "  make stop          -> Detener contenedores"
	@echo "  make test          -> Ejecutar tests con pytest"
