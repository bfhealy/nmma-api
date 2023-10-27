SHELL = /bin/bash
ESLINT=npx eslint

.DEFAULT_GOAL := help

PYTHON=PYTHONPATH=. python

SUPERVISORD_CFG=nmma_api/services/supervisor.conf
SUPERVISORD=$(PYTHON) -m supervisor.supervisord -s -c $(SUPERVISORD_CFG)
SUPERVISORCTL=$(PYTHON) -m supervisor.supervisorctl -c $(SUPERVISORD_CFG)

# Bold
B=\033[1m
# Normal
N=\033[0m

paths:
	@mkdir -p logs
	@mkdir -p logs/sv_child
	@mkdir -p run

dependencies: ## Install dependencies
	$(PYTHON) -m pip install -r requirements.txt --progress-bar off

summary:
	$(PYTHON) nmma_api/utils/config.py

validate_expanse_connection:
	$(PYTHON) nmma_api/tools/expanse.py

run: paths dependencies summary validate_expanse_connection ## Run the server in development mode
	$(SUPERVISORD)

run_production: paths summary validate_expanse_connection ## Run the server in production mode
	$(SUPERVISORD)

stop: ## Stop the server
	$(SUPERVISORCTL) stop all

monitor: ## Monitor the server
	$(SUPERVISORCTL) -i

log: paths ## Monitor log files for all services.
	@PYTHONPATH=. PYTHONUNBUFFERED=1 python nmma_api/utils/logs.py

docker_up: dependencies validate_expanse_connection ## Build docker image
	docker-compose up --build -d

docker_down: ## Stop docker image
	docker-compose down
