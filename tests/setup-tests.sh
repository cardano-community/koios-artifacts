#!/usr/bin/env bash

sudo apt-get install python3 python3-pip python3-virtualenv -y >/dev/null
export PATH="${HOME}"/.local/bin/:$PATH
virtualenv koios-tests >/dev/null
source koios-tests/bin/activate
python3 -m pip install schemathesis pytest-order >/dev/null
#curl -sfL https://raw.githubusercontent.com/cardano-community/koios-artifacts/main/tests/not_empty_response.py -o not_empty_response.py >/dev/null
export SCHEMATHESIS_HOOKS=not_empty_response

cat <<-EOF
	
	To run the endpoint validation tests, use the below:
	
	  export TOKEN="ey...."
	  schemathesis run --request-timeout 25000 --hypothesis-deadline 25000 ../specs/results/koiosapi-guild.yaml --hypothesis-phases=explicit --hypothesis-verbosity quiet \\
	      -b http://127.0.0.1:8053/api/v1 -c all --validate-schema=true -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" --experimental=openapi-3.1 --exclude-checks ignored_auth
	
	      where http://127.0.0.1:8053/api/v1 is the URL of instance you want to test, and guild.koios.rest is the target enviornment for testing.
	
	To run the data validations tests, use the below (WIP - skip for now):
	
	  pytest --local-url http://127.0.0.1:8053/api/v1 --compare-url https://guild.koios.rest/api/v1 --api-schema-file ../specs/results/koiosapi-guild.yaml -x -v
	
	  Arguments:
	      local-run		:	URL of instance you want to test"
	      compare-url	:	Source-of-truth instance to compare returned data against"
	      api-schema-file	:	The API specs/schema file you want to use as input for validation"
	
	To enter Python virtualenv, type 'source koios-tests/bin/activate'.
	To exit from Python virtualenv, you can run 'deactivate'.
	
	EOF
