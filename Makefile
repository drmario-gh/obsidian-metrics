PAGES_VAULT_PATH = "/Users/mlopez/Documents/second_brain/pages"
RAW_HIGHLIGHTS_PATH = "/Users/mlopez/Documents/second_brain/raw highlights"
ASSETS_VAULT_PATH = "/Users/mlopez/Documents/second_brain/assets"
UID := $(shell id -u)

.PHONY: start copy-vault-pages copy-stats-back

build:
	docker build -t custom-all-spark-notebook:2023-03-06 .

start:
	docker run --rm --name spark-notebook \
	  -p 8888:8888 \
	  -v "${PWD}":/home/jovyan/work \
	  --user ${UID} \
	  --group-add users \
	  custom-all-spark-notebook:2023-03-06

copy-vault-pages:
	cp -r ${PAGES_VAULT_PATH} .

copy-vault-raw-highlights:
	cp -r ${RAW_HIGHLIGHTS_PATH} .

compute-daily-stats:
	docker run --rm --name spark-notebook \
	  -p 8888:8888 \
	  -v "${PWD}":/home/jovyan/work \
	  --user ${UID} \
	  --group-add users \
	  -it custom-all-spark-notebook:2023-03-06  start.sh python work/main.py

copy-stats-back:
	cp assets/* ${ASSETS_VAULT_PATH}

daily-cronjob:
	make copy-vault-pages
	make copy-vault-raw-highlights
	make compute-daily-stats
	make copy-stats-back
