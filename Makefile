# --- GENERAL ---

.PHONY: clean requirements style 

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


# --- CONTAINERS ---

.PHONY: containers_init containers_build containers_up containers_down

containers_build:
	docker build \
		-f docker/airflow_docker_operator/Dockerfile \
		-t docker_image_task \
		.

containers_init:
	docker compose up airflow-init

containers_up:
	docker compose up -d

containers_down:
	docker compose down


# --- DATA FILES ---

.PHONY: data_interim data_features data

data/interim/%:
	python -m house_climate.data.preprocess	

data_interim: data/interim/%

data/processed/%: data_interim
	python -m house_climate.data.add_features

data_features: data/processed/%

data: data_interim data_features