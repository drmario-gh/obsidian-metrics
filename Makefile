PAGES_VAULT_PATH = "/Users/mlopez/Documents/second_brain/pages"
ASSETS_VAULT_PATH = "/Users/mlopez/Documents/second_brain/assets"
UID := $(shell id -u)

start:
	docker run --rm --name spark-notebook \
	  -p 8888:8888 \
	  -v "${PWD}":/home/jovyan/work \
	  --user ${UID} \
	  --group-add users \
	  jupyter/all-spark-notebook:2023-03-06

copy-vault-pages:
	cp -r ${PAGES_VAULT_PATH} .

copy-stats-back:
	cp assets/* ${ASSETS_VAULT_PATH}