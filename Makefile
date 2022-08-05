#!/usr/bin/env -S make -f

.PHONY: clean data lint references requirements help
.ONESHELL: clean data lint references requirements help

include .env


# Set env var value to the correct full path
build:
	PWD=`pwd`; cat .env.example | sed -E -e "s#__PWD__#${PWD}#" > .env;


# Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete


# Download dataset from source
data:
	wget $$URL_DATASET -P $$PATH_DATA_EXTERNAL;
	unzip -j "$$PATH_DATA_EXTERNAL/UCR_TimeSeriesAnomalyDatasets2021.zip" \
		"AnomalyDatasets_2021/UCR_TimeSeriesAnomalyDatasets2021/FilesAreInHere/UCR_Anomaly_FullData/*txt" \
		-d "$$PATH_DATA_RAW/UCR_Anomaly_FullData";


# Extract docs from source
references: data
	zipinfo -1 "$$PATH_DATA_EXTERNAL/UCR_TimeSeriesAnomalyDatasets2021.zip" -x "*txt" | \
		egrep -v "(\~|zip)" | \
		while read filename; do \
			unzip "$$PATH_DATA_EXTERNAL/UCR_TimeSeriesAnomalyDatasets2021.zip" "$$filename" \
				-d "$$PATH_DOCS_REFERENCES/UCR"; \
		done;
	cd "$$PATH_DOCS_REFERENCES/UCR"; \
		mv ./AnomalyDatasets_2021/UCR_TimeSeriesAnomalyDatasets2021/* .; \
		rm -rf ./AnomalyDatasets_2021;


# Set up Python development environment
requirements: build
	python3 -m venv ./venv
	source ./venv/bin/activate
	pip install -U pipenv
	pipenv shell
	pipenv sync
	pre-commit install -f


update-repos:
	git push origin main
	git push github main


#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

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
