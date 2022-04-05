#!/usr/bin/env bash

sudo apt-get install python3 python3-pip -y >/dev/null
python3 -m pip install virtualenv >/dev/null
export PATH=$PATH:~/.local/bin/
virtualenv koios-tests >/dev/null
source koios-tests/bin/activate
python3 -m pip install schemathesis pytest-order >/dev/null
curl -sfL https://raw.githubusercontent.com/cardano-community/koios-artifacts/master/tests/not_empty_response.py -o not_empty_response.py >/dev/null

cat <<-EOF
	
	To run the endpoint validation tests, use the below:
	
	  schemathesis --pre-run not_empty_response run --request-timeout 5000 --hypothesis-seed 1 https://guild.koios.rest/koiosapi.yaml \\
	               --hypothesis-phases=explicit -v --hypothesis-verbosity quiet -b http://127.0.0.1:8053/api/v0 -c all
	
	      where http://127.0.0.1:8053/api/v0 is the URL of instance you want to test, and guild.koios.rest is the target enviornment for testing
	
	To run the data validations tests, use the below:
	
	  pytest --local-url http://127.0.0.1:8053/api/v0 --compare-url https://guild.koios.rest/api/v0 --api-schema-file ../specs/results/koiosapi-guild.yaml -x -v
	
	  Arguments:
	      local-run		:	URL of instance you want to test"
	      compare-url	:	Source-of-truth instance to compare returned data against"
	      api-schema-file	:	The API specs/schema file you want to use as input for validation"
	
	To enter Python virtualenv, type 'source koios-tests/bin/activate'
	To exit from Python virtualenv, you can run 'deactivate' 
	
	EOF
