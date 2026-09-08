from __future__ import annotations

from pathlib import Path
from typing import Any

from openapi_core import OpenAPI
from openapi_core.contrib.requests import RequestsOpenAPIRequest, RequestsOpenAPIResponse
from openapi_spec_validator import validate_spec
from openapi_spec_validator.readers import read_from_filename


class ContractValidator:
    def __init__(self, spec_path: Path) -> None:
        self.spec_path = spec_path
        self.openapi = OpenAPI.from_file_path(str(spec_path))

    def request_errors(self, prepared_request: Any) -> list[str]:
        request = RequestsOpenAPIRequest(prepared_request)
        return [str(error) for error in self.openapi.iter_request_errors(request)]

    def response_errors(self, prepared_request: Any, response: Any) -> list[str]:
        request = RequestsOpenAPIRequest(prepared_request)
        openapi_response = RequestsOpenAPIResponse(response)
        return [
            str(error)
            for error in self.openapi.iter_response_errors(request, openapi_response)
        ]


def validate_document(spec_path: Path) -> None:
    document, _ = read_from_filename(str(spec_path))
    validate_spec(document)
