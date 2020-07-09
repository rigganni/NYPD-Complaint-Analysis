#################################################################################
# GLOBALS                                                                       #
#################################################################################


#################################################################################
# COMMANDS                                                                      #
#################################################################################

.PHONY: requirements
## Install Python Dependencies
requirements: test_environment
	@echo ">>> Creating conda environment nypd-complaint-analysis"
	conda env create -f conda.yml


.PHONY: clean
## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

.PHONY: airflow_trigger_dag_local
## Execute airflow on local dev environment
airflow_trigger_dag_local:
	docker exec -it webserver-airflow sh -c "/entrypoint.sh airflow trigger_dag nypd_complaint_analysis --conf '{\"env\":\"local\"}'"

.PHONY: airflow_trigger_dag_aws
## Execute airflow on AWS
airflow_trigger_dag_aws:
	docker exec -it webserver-airflow sh -c "/entrypoint.sh airflow trigger_dag nypd_complaint_analysis --conf '{\"env\":\"aws\"}'"

.PHONY: airflow_deploy
## Deploy airflow file
airflow_deploy:
	cp src/nypd_complaint_airflow.py ~/docker/airflow-webserver/dags/.

.PHONY: airflow_clear_runs
## Clear all airflow runs
airflow_clear_runs:
	docker exec -it webserver-airflow sh -c "/entrypoint.sh airflow clear -c nypd_complaint_analysis"

.PHONY: run_local_transform
## Run local Spark transforms
run_local_transform:
	docker cp src/transform_data.py spark:/tmp/.
	docker cp dl.cfg spark:/tmp/.
	docker exec -it spark sh -c "python /tmp/transform_data.py"

.PHONY: spark_analysis
## Open interactive prompt for Spark ad hoc analysis
spark_analysis:
	docker cp src/interactive_analysis.py spark:/tmp/.
	docker exec -it spark sh -c "python -i /tmp/interactive_analysis.py"

.PHONY: lint
## Lint all python files
lint:
	yapf -i src/load_data.py
	yapf -i src/nypd_complaint_airflow_etl.py
	yapf -i src/transform_data.py

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# From: https://github.com/drivendata/cookiecutter-data-science/blob/master/%7B%7B%20cookiecutter.repo_name%20%7D%7D/Makefile
# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
