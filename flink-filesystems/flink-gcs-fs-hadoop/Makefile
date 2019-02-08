.PHONY: help test
.DEFAULT_GOAL=help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test: ## Run IT test
	 mvn test -Dtest=HadoopGCSFileSystemITCase

deps: ## show dependencies
	 mvn dependency:tree
