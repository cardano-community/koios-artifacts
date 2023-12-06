#!/usr/bin/env python3

import json, pathlib, traceback

inputspecsfile="templates/api-main.yaml"
apiinfofile="templates/1-api-info.yaml"
apiparamsfile="templates/2-api-params.yaml"
apirequestBodiesfile="templates/3-api-requestBodies.yaml"
apischemafile="templates/4-api-schemas.yaml"
examplesfile="templates/example-map.json"
examples=json.loads(pathlib.Path(examplesfile).read_text())

def populate_spec(network, outf):
  """Populate template spec using network initial and write to outf file"""
  template = pathlib.Path(inputspecsfile).read_text()
  template=template.replace("#!info!#", str(pathlib.Path(apiinfofile).read_text()).rstrip())
  template=template.replace("#!params!#", str(pathlib.Path(apiparamsfile).read_text()).rstrip())
  template=template.replace("#!requestBodies!#", str(pathlib.Path(apirequestBodiesfile).read_text()).rstrip())
  template=template.replace("#!schemas!#", str(pathlib.Path(apischemafile).read_text()).rstrip())
  print("Creating " + outf + " using koiosapi.yaml as template...")
  for ep in examples["params"]:
    template=template.replace(str("##" + str(ep) + "_param##"), str(examples["params"][ep][str(network)]))
  for erb in examples["requestBodies"]:
    template=template.replace("##" + str(erb) + "_rb##", str(examples["requestBodies"][erb][str(network)]))
  with open(outf, 'w') as f:
    f.write(template)

def main():
  populate_spec("m", "results/koiosapi-mainnet.yaml")
  populate_spec("g", "results/koiosapi-guild.yaml")
  populate_spec("pv", "results/koiosapi-preview.yaml")
  populate_spec("pp", "results/koiosapi-preprod.yaml")
  print("Done!!")

try:
  main()
except Exception as e:
  print("Error occured : " + str(e) + "\n Traceback : " + str(traceback.print_exc()))
