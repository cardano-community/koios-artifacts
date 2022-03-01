#!/usr/bin/env python3

import json, pathlib, traceback

inputspecsfile="koiosapi.yaml"
examplesfile="spec_examples.json"
examples=json.loads(pathlib.Path(examplesfile).read_text())

def populate_spec(network, outf):
  """Populate template spec using network initial and write to outf file"""
  template = pathlib.Path(inputspecsfile).read_text()
  print("Creating " + outf + " using koiosapi.yaml as template...")
  for e in examples["params"]:
    template=template.replace(str("##" + str(e) + "_param##"), str(examples["params"][e][str(network)]))
  for e in examples["requestBodies"]:
    template=template.replace("##" + str(e) + "_rb##", str(examples["requestBodies"][e][str(network)]))
  with open(outf, 'w') as f:
    f.write(template)

def main():
  populate_spec("m", "koiosapi-mainnet.yaml")
  populate_spec("t", "koiosapi-testnet.yaml")
  populate_spec("g", "koiosapi-guild.yaml")
  print("Done!!")

try:
  main()
except Exception as e:
  print("Error occured : " + str(e) + "\n Traceback : " + str(traceback.print_exc()))
