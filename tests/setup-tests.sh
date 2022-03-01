#!/usr/bin/env bash

python3 -m pip install virtualenv
export PATH=$PATH:~/.local/bin/
virtualenv koios-tests
source koios-tests/bin/activate
pip3 install schemathesis
echo " To run the current test, use the below:"
echo "    schemathesis --pre-run not_empty_response run --request-timeout 5000 --hypothesis-seed 1 https://guild.koios.rest/koiosapi.yaml --hypothesis-phases=explicit -v --hypothesis-verbosity quiet -b http://127.0.0.1:8053/api/v0 -c not_empty_response"
echo "        where http://127.0.0.1:8053/api/v0 is the URL of instance you want to test, and guild.koios.rest is the enviornment you want to test against" 
echo "    *[Not Ready] - Schema output format matching is not yet ready, but if you'd like to still run all the tests, you can use '-c all' in the above command"

echo " To quit from Python virtualenv, you can run 'deactivate' "
