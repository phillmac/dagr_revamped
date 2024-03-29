import logging
from os import environ
from pathlib import Path
from shutil import rmtree
from time import sleep

import docker
from dagr_revamped.builtin_plugins.classes.DAGRHTTPIo import DAGRHTTPIo
from dagr_revamped.config import DAGRConfig
from dagr_revamped.DAGRIo import DAGRIo

logging.basicConfig(format='%(levelname)s:%(message)s', level=5)

config = DAGRConfig(
    include=[Path(__file__).parent])

client = docker.from_env()
image_tag = environ.get('IMAGE_TAG', 'latest')
image = f"phillmac/dagr_selenium:{image_tag}"
client.images.pull(image)

io_items = []

def run_container(output_dir=None):
    return client.containers.run(
        image=image,
        command=["-u", "-m", "dagr_selenium.filenames_server"],
        detach=True,
        environment={},
        init=True,
        ports={
            '3002/tcp': 3002
        },
        user='root',
        volumes={
            output_dir: {"bind": output_dir, "mode": "rw"}
        },
        working_dir=output_dir
    )


def select_io_class():
    classes_mapping = {
        'http': DAGRHTTPIo,
        'default': DAGRIo
    }

    env_io_class = environ.get('TEST_IO_CLASS', 'default')

    if env_io_class in classes_mapping:
        return classes_mapping[env_io_class]

    raise Exception(f"Unknown io class '{env_io_class}'")


def create_io(testcase, io, base_dir=None, rel_dir=None):
    result = io.create(base_dir or testcase.results_dir, rel_dir or '', config)
    io_items.append(result)
    return result


def setUpTestCase(testcase):
    testcase.results_dir = Path(__file__, '../../test_results').resolve()

    if not testcase.results_dir.exists():
        testcase.results_dir.mkdir()

    testcase.container = run_container(str(testcase.results_dir))
    sleep(3)


def tearDownTestCase(testcase):
    for io in io_items:
        io.close()

    io_items.clear()

    testcase.container.stop()
    testcase.container.remove()
    if environ.get('DISABLE_DIR_CLEANUP', 'FALSE') != 'TRUE':
        rmtree(testcase.results_dir)
