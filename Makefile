#################################################################################
# GLOBALS                                                                       #
#################################################################################


#################################################################################
# COMMANDS                                                                      #
#################################################################################

.PHONY: requirements
## Install Python Dependencies
requirements:
	@echo ">>> Creating conda environment nypd-complaint-analysis"
	conda env create -f conda.yml


.PHONY: clean
## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	docker exec -it ${AIRFLOW_DOCKER_ID} sh -c "/entrypoint.sh airflow delete_dag nypd_complaint_analysis"
	rm -ri ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint

.PHONY: cleanup_aws
## Remove bastion host & RedShift cluster
cleanup_aws:
	python src/cleanup_cluster.py

.PHONY: airflow_trigger_dag_local
## Execute airflow on local dev environment
airflow_trigger_dag_local:
	docker exec -it ${AIRFLOW_DOCKER_ID} sh -c "/entrypoint.sh airflow trigger_dag nypd_complaint_analysis --conf '{\"env\":\"local\"}'"

.PHONY: airflow_trigger_dag_aws_keep_redshift
## Execute airflow on AWS and keep RedShift cluster
airflow_trigger_dag_aws_keep_redshift:
	docker exec -it ${AIRFLOW_DOCKER_ID} sh -c "/entrypoint.sh airflow trigger_dag nypd_complaint_analysis --conf '{\"env\":\"aws\", \"delete_redshift_cluster\":\"False\"}'"

.PHONY: airflow_trigger_dag_aws_delete_redshift
## Execute airflow on AWS and delete RedShift cluster
airflow_trigger_dag_aws_delete_redshift:
	docker exec -it ${AIRFLOW_DOCKER_ID} sh -c "/entrypoint.sh airflow trigger_dag nypd_complaint_analysis --conf '{\"env\":\"aws\", \"delete_redshift_cluster\":\"True\"}'"

.PHONY: airflow_deploy
## Deploy airflow file
airflow_deploy:
	mkdir -p ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/nypd_complaint_airflow.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/transform_data.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/create_redshift_cluster_database.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/redshift.cfg ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/load_data_to_redshift.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/create_bastion_host.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/cleanup_cluster.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/sql_queries.py ${AIRFLOW_DAG_ROOT_FOLDER}/.
	cp src/transform_data.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/create_redshift_cluster_database.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/redshift.cfg ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/load_data_to_redshift.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/create_bastion_host.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/cleanup_cluster.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	cp src/sql_queries.py ${AIRFLOW_DAG_ROOT_FOLDER}/nypd-complaint/.
	docker cp src/redshift.cfg ${AIRFLOW_DOCKER_ID}:/tmp/.
	docker cp src/bastion.cfg ${AIRFLOW_DOCKER_ID}:/tmp/.
	docker cp ${AWS_EMR_SSH_IDENTITY_FILE} ${AIRFLOW_DOCKER_ID}:/tmp/pkey.pem

.PHONY: airflow_get_configs
## Download configparser files from AirFlow to local environment
airflow_get_configs:
	docker cp ${AIRFLOW_DOCKER_ID}:/tmp/bastion.cfg /tmp/.
	docker cp ${AIRFLOW_DOCKER_ID}:/tmp/redshift.cfg /tmp/.

airflow_deploy:
.PHONY: airflow_clear_runs
## Clear all airflow runs
airflow_clear_runs:
	docker exec -it ${AIRFLOW_DOCKER_ID} sh -c "/entrypoint.sh airflow clear -c nypd_complaint_analysis"

.PHONY: run_local_transform
## Run local Spark transforms
run_local_transform:
	docker cp src/transform_data.py ${SPARK_LOCAL_MASTER_DOCKER_ID}:/tmp/.
	docker cp local.cfg ${SPARK_LOCAL_MASTER_DOCKER_ID}:/tmp/.
	docker exec -it ${SPARK_LOCAL_MASTER_DOCKER_ID} sh -c "python /tmp/transform_data.py 'local'"

.PHONY: spark_analysis_local
## Open interactive prompt for Spark ad hoc analysis on local Spark docker container
spark_analysis_local:
	docker cp src/interactive_analysis.py ${SPARK_LOCAL_MASTER_DOCKER_ID}:/tmp/.
	docker exec -it ${SPARK_LOCAL_MASTER_DOCKER_ID} sh -c "python -i /tmp/interactive_analysis.py local"

.PHONY: spark_analysis_aws
## Open interactive prompt for Spark ad hoc analysis on AWS EMR cluster
spark_analysis_aws:
	$(eval emr_id=$(shell sh -c "aws emr list-clusters --region us-west-2 --active | jq '[.Clusters | .[] | .Id][0]'")) 
	@echo $(emr_id)
	$(eval dns=$(shell sh -c "aws emr describe-cluster --cluster-id $(emr_id) --region us-west-2 --query Cluster.MasterPublicDnsName"))
	@echo $(dns)
	scp -i ${AWS_EMR_SSH_IDENTITY_FILE} -o StrictHostKeyChecking=no src/interactive_analysis.py hadoop@$(dns):/tmp/.
	ssh -i ${AWS_EMR_SSH_IDENTITY_FILE} -o StrictHostKeyChecking=no hadoop@$(dns) "/usr/bin/python3 -i /tmp/interactive_analysis.py aws"

.PHONY: test_transform_aws
## Test transform_data.py on existing EMR instance on AWS
test_transform_aws:
	$(eval emr_id=$(shell sh -c "aws emr list-clusters --region us-west-2 --active | jq '[.Clusters | .[] | .Id][0]'")) 
	@echo $(emr_id)
	$(eval dns=$(shell sh -c "aws emr describe-cluster --cluster-id $(emr_id) --region us-west-2 --query Cluster.MasterPublicDnsName"))
	@echo $(dns)
	scp  -i ${AWS_EMR_SSH_IDENTITY_FILE} -o StrictHostKeyChecking=no src/transform_data.py hadoop@$(dns):/tmp/.
	aws emr add-steps --region us-west-2 --cluster-id $(emr_id) --steps Type="CUSTOM_JAR",Name="Test Transforms",Jar="command-runner.jar",ActionOnFailure="CONTINUE",Args="['sudo', '-H', '-u', 'hadoop', 'bash', '-c', \"/usr/bin/python3 /tmp/transform_data.py aws ${AWS_ACCESS_KEY} ${AWS_SECRET_ACCESS_KEY}\"]"

# Source: https://stackoverflow.com/questions/53382383/makefile-cant-use-conda-activate
# Need to specify bash in order for conda activate to work.
SHELL=/bin/bash
# Note that the extra activate is needed to ensure that the activate floats env to the front of PATH
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate
CONDA_DEACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda deactivate ; conda deactivate
.PHONY: test_redshift_aws_creation
## Test creating of RedShift cluster
test_redshift_aws_creation:
	($(CONDA_ACTIVATE) nypd-complaint-analysis; cd src; python create_redshift_cluster_database.py; $(CONDA_DEACTIVATE) nypd-complaint-analysis)

.PHONY: connect_psql_redshift
## Connect to nypd_complaint via psql
connect_psql_redshift:
	$(eval public_dns=$(shell sh -c "grep PUBLIC_DNS /tmp/bastion.cfg | cut -d' ' -f 3"))
	$(eval redshift_host=$(shell sh -c "grep DWH_HOST /tmp/redshift.cfg | cut -d' ' -f 3"))
	$(eval db_user=$(shell sh -c "grep DWH_DB_USER /tmp/redshift.cfg | cut -d' ' -f 3"))
	$(eval db_password=$(shell sh -c "grep DWH_DB_PASSWORD /tmp/redshift.cfg | cut -d' ' -f 3"))
	$(eval db_port=$(shell sh -c "grep DWH_PORT /tmp/redshift.cfg | cut -d' ' -f 3"))
	ssh -D localhost:8087 -S /tmp/.ssh-aws-bastion -M -o StrictHostKeyChecking=no -i ${AWS_EMR_SSH_IDENTITY_FILE} ec2-user@$(public_dns) -fNT -L 5439:$(redshift_host):5439
	PGPASSWORD=$(db_password) psql -h 127.0.0.1 -d nypd_complaint -U $(db_user) -p $(db_port)
	ssh -S /tmp/.ssh-aws-bastion -O exit $(public_dns)

.PHONY: ssh_aws_emr_master
## Connect to master node of running AWS EMR cluster
ssh_aws_emr_master:
	$(eval emr_id=$(shell sh -c "aws emr list-clusters --region us-west-2 --active | jq '[.Clusters | .[] | .Id][0]'")) 
	@echo $(emr_id)
	$(eval dns=$(shell sh -c "aws emr describe-cluster --cluster-id $(emr_id) --region us-west-2 --query Cluster.MasterPublicDnsName"))
	@echo $(dns)
	ssh -i ${AWS_EMR_SSH_IDENTITY_FILE} -o StrictHostKeyChecking=no hadoop@$(dns)


.PHONY: mount_aws_emr_log_directory
## Mount AWS EMR logs via s3fs to local directory
mount_aws_emr_log_directory:
	mkdir -p /tmp/aws-emr-logs
	$(eval user_id=$(shell sh -c "id -u"))
	$(eval user_group_id=$(shell sh -c "id -g"))
	s3fs ${AWS_EMR_LOG_BUCKET} /tmp/aws-emr-logs -o passwd_file=${AWS_S3_S3FS_PASSWORD_FILE} -o allow_other,uid=$(user_id),gid=$(user_group_id) -o umask=0007

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
