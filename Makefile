.PHONY: clean requirements style 
.PHONY: containers_init containers_custom containers_up containers_down


## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete


## Install Python Dependencies
requirements:
	python -m pip install -r requirements.txt


## Apply formatting
style:
	black ./dags
	isort ./dags ./src --profile black

containers_init:
	docker compose up airflow-init

containers_custom:
	docker build \
		-f docker/airflow_docker_operator/Dockerfile \
		-t docker_image_task \
		. && \

containers_up:
	docker compose up -d

containers_down:
	docker compose down