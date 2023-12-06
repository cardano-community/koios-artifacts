#!/usr/bin/env python3

import schemathesis

#DEFAULT_CHECKS: Tuple["CheckFunction", ...] = (not_a_server_error,)

@schemathesis.register_check
def not_empty_response(response, case):
  if response.status_code != 200 or response.text == "[]":
    raise AssertionError("Error occurred ---> status_code: " + str(response.status_code) + ", text returned : " + str(response.text) )
