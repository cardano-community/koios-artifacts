import pytest
import schemathesis


def pytest_addoption(parser):
    parser.addoption(
        "--local-url", action="store", default="http://127.0.0.1:8453/api/v0/"
    )
    parser.addoption(
        "--compare-url", action="store", default="https://guild.koios.rest/api/v0/"
    )
    parser.addoption(
        "--api-schema-file",
        action="store",
        default="../specs/results/koiosapi-guild.yaml",
    )


@pytest.fixture
def local_url(request):
    return request.config.getoption("--local-url")


@pytest.fixture
def compare_url(request):
    return request.config.getoption("--compare-url")


@pytest.fixture
def api_schema(request):
    schema = schemathesis.from_path(request.config.getoption("--api-schema-file"))

    return schema
