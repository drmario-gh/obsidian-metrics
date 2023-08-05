VAULT_PATH = "/mnt/c/Users/mario/Documents/second_brain"
PAGES_VAULT_PATH = "${VAULT_PATH}/pages"
RAW_HIGHLIGHTS_PATH = "${VAULT_PATH}/raw highlights"
ASSETS_VAULT_PATH = "${VAULT_PATH}/assets"
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
	rm -rf pages
	cp -r ${PAGES_VAULT_PATH} .

copy-vault-raw-highlights:
	rm -rf raw\ highlights
	# Without any change, I started having this error. It seems to be a WSL2 issue and corruption of the file system.
	# cp: cannot access '/mnt/c/Users/mario/Documents/second_brain/raw highlights/Articles': Input/output error
	# I don't feel like solving this now, and I can live with this workaround.
	# I checked the number of files in the folder and it's the same as in the original folder.
	cp -r ${RAW_HIGHLIGHTS_PATH} . || true
	cp -r ${RAW_HIGHLIGHTS_PATH}/Articles/* raw\ highlights/Articles

compute-daily-stats:
	docker run --rm --name spark-notebook \
	  -p 8888:8888 \
	  -v "${PWD}":/home/jovyan/work \
	  --user ${UID} \
	  --group-add users \
	  -it custom-all-spark-notebook:2023-03-06  start.sh python work/main.py

compute-daily-stats-no-save:
	docker run --rm --name spark-notebook \
	  -p 8888:8888 \
	  -v "${PWD}":/home/jovyan/work \
	  --user ${UID} \
	  --group-add users \
	  -it custom-all-spark-notebook:2023-03-06  start.sh python work/main.py --no-save


copy-stats-back:
	cp assets/* ${ASSETS_VAULT_PATH}

daily-cronjob:
	make copy-vault-pages
	make copy-vault-raw-highlights
	make compute-daily-stats
	make copy-stats-back

run-no-save:
	make copy-vault-pages
	make copy-vault-raw-highlights
	make compute-daily-stats-no-save
	make copy-stats-back
