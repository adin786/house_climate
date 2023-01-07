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