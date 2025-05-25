#!/usr/bin/env python3

import json, pathlib, traceback, re, textwrap

inputspecsfile="templates/api-main.yaml"
apiinfofile="templates/1-api-info.yaml"
apiparamsfile="templates/2-api-params.yaml"
apirequestBodiesfile="templates/3-api-requestBodies.yaml"
apischemafile="templates/4-api-schemas.yaml"
examplesfile="templates/example-map.json"
examples=json.loads(pathlib.Path(examplesfile).read_text())

PLACEHOLDER_MAP = {
    "info":         pathlib.Path(apiinfofile).read_text().rstrip(),
    "params":       pathlib.Path(apiparamsfile).read_text().rstrip(),
    "requestBodies":pathlib.Path(apirequestBodiesfile).read_text().rstrip(),
    "schemas":      pathlib.Path(apischemafile).read_text().rstrip(),
}

_PH_RE = re.compile(
    r"^(?P<indent>[ \t]*)#!(?P<key>"
    + "|".join(re.escape(k) for k in PLACEHOLDER_MAP)
    + r")!#",
    flags=re.MULTILINE,
)

def _inject(match: re.Match) -> str:
    indent = match.group("indent")
    key    = match.group("key")
    block  = PLACEHOLDER_MAP[key]

    return textwrap.indent(block, indent)

def populate_spec(network, outf):
  """Populate template spec using network initial and write to outf file"""
  template = pathlib.Path(inputspecsfile).read_text()
  template = _PH_RE.sub(_inject, template)
  template=template.replace("#!info!#", str(pathlib.Path(apiinfofile).read_text()).rstrip())
  template=template.replace("#!params!#", str(pathlib.Path(apiparamsfile).read_text()).rstrip())
  template=template.replace("#!requestBodies!#", str(pathlib.Path(apirequestBodiesfile).read_text()).rstrip())
  template=template.replace("#!schemas!#", str(pathlib.Path(apischemafile).read_text()).rstrip())
  print("Creating " + outf + " using koiosapi.yaml as template...")
  for ep, vals in examples["params"].items():
      template = template.replace(f"##{ep}_param##", vals[network])
  for erb, vals in examples["requestBodies"].items():
      template = template.replace(f"##{erb}_rb##", vals[network])
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
