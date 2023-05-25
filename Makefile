.PHONY: clean-venv
clean-venv:
	@rm -rf venv

.PHONY: init-venv
init-venv: clean-venv
	pip3 install virtualenv; \
	virtualenv venv --python=python3.8; \
	. ./venv/bin/activate; \
	pip3 install -r requirements.txt;
