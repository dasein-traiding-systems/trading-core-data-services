PROJECT_DIR := "trading-core-data-services"
SSH_KEY_FILE := ~/.ssh/id_rsa
MAIN_SERVER := 5.75.137.107

.PHONY: help manage docker containers .EXPORT_ALL_VARIABLES
.DEFAULT_GOAL := help

help:  ## 💬 This help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

rebuild:  ## make rebuild service=....
	ssh -o StrictHostKeyChecking=no -i $(SSH_KEY_FILE) root@$(MAIN_SERVER) " cd \"$(PROJECT_DIR)\" && docker-compose up --build --force-recreate --no-deps -d \"$(service)\""

restart:  ## make rebuild service=....
	ssh -o StrictHostKeyChecking=no -i $(SSH_KEY_FILE) root@$(MAIN_SERVER) " cd \"$(PROJECT_DIR)\" && docker-compose restart \"$(service)\""

deploy:  ## make rebuild service=....
	ssh -o StrictHostKeyChecking=no -i $(SSH_KEY_FILE) root@$(MAIN_SERVER) " cd $(PROJECT_DIR) && docker-compose up --build --force-recreate --no-deps -d"

