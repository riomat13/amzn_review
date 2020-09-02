# Usage:
# make				# setup
# make init 		# install requirements
# make local 		# setup environment for local execution
# make run-worker  	# start airflow
# make stop-worker 	# stop airflow
# make test 		# run unittests

PE := pipenv run

all: install init
local: install init-local
install:
	pipenv install -e .
resetdb: clear init
resetdb-local: clear-local init-local
init:
	${PE} workflow-init
init-local:
	${PE} workflow-init-local
clear-logs:
	rm -rf workflow/logs/*
	rm -f logs/*.log
clear:
	${PE} workflow-stop
	docker stop amzn_review_airflow
	docker stop amzn_review_redis
clear-local:
	${PE} workflow-stop
	docker stop amzn_review_db
	docker stop amzn_review_airflow
	docker stop amzn_review_redis
start-worker:
	${PE} workflow-run
stop-worker:
	${PE} workflow-stop
start-uploader:
	${PE} amzn-review uploader run
start-uploader-local:
	${PE} amzn-review uploader run --local
test:
	${PE} pip install -q -e .
	${PE} flake8 --ignore=E501 amzn_review/
	${PE} flake8 --ignore=E501 workflow/
	${PE} coverage run -m unittest
	${PE} coverage report -m
help:
	@echo "Usage:"
	@echo "  make:                      setup and start airflow"
	@echo "  make local:                setup and start airflow for local environment"
	@echo "  make install:              install requirements"
	@echo "  make init:                 setup environment for local execution"
	@echo "  make resetdb:              reset all database for airflow pipelining tasks"
	@echo "  make clear:                remove all database and logs for \`workflow\`"
	@echo "  make init-local:           setup environment for local execution"
	@echo "  make resetdb-local:        reset all database for airflow pipelining tasks in local environment"
	@echo "  make clear-local:          remove all database and logs for \`workflow\` in local environment"
	@echo "  make startrt-worker:       start airflow"
	@echo "  make stop-worker:          stop airflow"
	@echo "  make start-uploader:       start run data uploader to AWS S3 bucket"
	@echo "  make start-uploader-local: start run data uploader for local environment"
