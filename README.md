# NYC Crime Analysis

An analysis of NYC crime using open data sources.

#### Sources

* NYC Crime Data
  * https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
* Great article on creating reproducible data science datasets
  * Inspiration for utilizing Makefile
  * https://medium.com/@davidstevens_16424/make-my-day-ta-science-easier-e16bc50e719c

#### Future Improvements

* Execute Spark code from Airflow
  * Local Airflow service is running in a container. Currently limited by not being to able to call Docker container from another within another container. Could create an API (i.e. gunicorn & flask) or just a hook to call `make run_transform_local`.
  * Out of scope for this project currently
* Create Ansible playbook to install & setup all dependencies
  * Install local Airflow instance
  * Install local Spark cluster
* Adjust my docker-compose files for Airflow & Spark and post to repository
  * Currently hardcoded values in docker-compose and bash scripts for my specific environment
  * Traefik is used as reverse proxy and would require quite a bit of documentation for user customization
  * Creating an Ansible playbook for setup possibly the answer
