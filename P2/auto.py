import os.path
import subprocess
import time
import json
import requests

from lib.autobadger import Callback, Autobadger, AutobadgerCallback
from lib.enums import Project, Registry
from lib.exceptions import MissingRequiredResource
from lib.io import IOTargetWrapper, DockerBuildTargetWrapper, DockerComposeTargetWrapper
from lib.registry import register, graded
from lib.runnable import TestRunnable
from lib.types import TestError


@register(project=Project.P2, registry=Registry.TEST, points=100)
class ProjectTwoTest(TestRunnable):
    def __init__(self, callback: "AutobadgerCallback", delegate: "Autobadger"):
        super().__init__(callback, delegate)
        self.containers = []
        self.env = {"PROJECT": self.delegate.grade_id, **dict(os.environ)}

    @staticmethod
    def build_docker(dockerfile, image) -> int | TestError:
        if not os.path.exists(IOTargetWrapper(dockerfile)):
            return TestError(
                message=f"{dockerfile} does not exist.",
                earned=0,
            )
        try:
            output = subprocess.run(
                DockerBuildTargetWrapper(t=image, f=dockerfile),
                capture_output=True,
                timeout=60 * 5,
            )
        except subprocess.TimeoutExpired:
            return TestError(message="`docker build` timed out.", earned=0)
        if output.returncode != 0:
            return TestError(
                message=f"Failed to build docker image: {output.stderr}", earned=0
            )
        return 5

    @graded(Q=1, points=5)
    def test_docker_build_dataset(self) -> int | TestError:
        return self.build_docker(
            "Dockerfile.dataset", f"{self.delegate.grade_id}-dataset"
        )

    @graded(Q=2, points=5)
    def test_docker_build_cache(self) -> int | TestError:
        return self.build_docker("Dockerfile.cache", f"{self.delegate.grade_id}-cache")

    @graded(Q=3, points=10)
    def test_compose_run(self) -> int | TestError:
        if not os.path.exists(IOTargetWrapper("docker-compose.yml")):
            return TestError(
                message="docker-compose.yml does not exist.",
                earned=0,
            )
        try:
            output = subprocess.run(
                DockerComposeTargetWrapper(d="", subcommand="up"),
                env=self.env,
                capture_output=True,
                timeout=60 * 5,
            )
        except subprocess.TimeoutExpired:
            return TestError(message="`docker compose up` timed out.", earned=0)
        if output.returncode != 0:
            return TestError(
                message=f"Failed to bring compose up: {output.stderr}", earned=0
            )

        # are the containers still up after 500ms?
        time.sleep(0.5)
        try:
            output = subprocess.run(
                DockerComposeTargetWrapper(subcommand="ps", format="json"),
                env=self.env,
                capture_output=True,
                timeout=60 * 5,
            )
        except subprocess.TimeoutExpired:
            return TestError(message="`docker compose ps` timed out.", earned=0)
        if output.returncode != 0:
            return TestError(
                message=f"Failed to run docker compose ps: {output.stderr}", earned=0
            )
        lines = output.stdout.strip().splitlines()
        container_count = len(lines)
        if container_count != 5:
            return TestError(
                message=f"Expected 5 containers to be running but found {container_count}.",
                earned=0,
            )
        self.containers = list(map(json.loads, sorted(lines)))
        return 10

    @graded(Q=4, points=10)
    def test_simple_http(self) -> int | TestError:
        address = self._test_cache_server("-cache-1")
        if isinstance(address, TestError):
            return address
        r = requests.get(f"{address}/lookup/53706")
        r.raise_for_status()
        result = r.json()
        if "addrs" not in result or "source" not in result:
            return TestError(
                message=f"Result body should be JSON with 'addrs' and 'source' fields, but got {result}.",
                earned=5,
            )

        return 10

    def _test_cache_server(self, server_suffix: str) -> str | TestError:
        cache_server = [c for c in self.containers if c["Name"].endswith(server_suffix)]
        if len(cache_server) != 1:
            return TestError(
                message=f"Expected to find 1 container named *{server_suffix} but found {cache_server}.",
                earned=0,
            )
        server_port = cache_server[0]["Publishers"][0]["PublishedPort"]
        return f"http://localhost:{server_port}"

    @graded(Q=5, points=10)
    def test_load_balance(self) -> int | TestError:
        address = self._test_cache_server("-cache-2")
        if isinstance(address, TestError):
            return address
        sources = []
        for i in range(6):
            # cache entries are at most 8 addrs, so this should always go to a dataset server
            r = requests.get(f"{address}/lookup/53706?limit=9")
            r.raise_for_status()
            sources.append(r.json()["source"])
        expected = ["1", "2", "1", "2", "1", "2"]
        if sources != expected:
            return TestError(
                message=f"Expected sources {expected} but got {sources}.",
                earned=0,
            )

        return 10

    @graded(Q=6, points=15)
    def test_limit(self) -> int | TestError:
        address = self._test_cache_server("-cache-2")
        if isinstance(address, TestError):
            return address
        # populate cache entry
        r = requests.get(f"{address}/lookup/53704?limit=100")
        r.raise_for_status()
        if len(r.json()["addrs"]) != 100:
            return TestError(
                message=f'Expected 100 addrs but got {len(r.json()["addrs"])}.',
                earned=0,
            )

        expected = [
            "1 Anniversary Ct",
            "1 Basil Ct",
            "1 Birchwood Cir",
            "1 Buhler Ct",
            "1 Burning Wood Ct",
            "1 Cherokee Cir Unit 101",
            "1 Cherokee Cir Unit 102",
            "1 Cherokee Cir Unit 103",
            "1 Cherokee Cir Unit 104",
            "1 Cherokee Cir Unit 201",
        ]

        for limit in range(1, 11):
            url = f"{address}/lookup/53704?limit={limit}"
            r = requests.get(url)
            r.raise_for_status()
            result = r.json()

            if result["addrs"] != expected[:limit]:
                return TestError(
                    message=f'For {url}, expected {expected[:limit]} but got {result["addrs"]}.',
                    earned=0,
                )

            if limit <= 8:
                if result["source"] != "cache":
                    return TestError(
                        message=f"Results for {url} should have been from the cache.",
                        earned=0,
                    )
            else:
                if result["source"] == "cache":
                    return TestError(
                        message=f"Results for {url} should NOT have been from the cache.",
                        earned=0,
                    )

        return 15

    @graded(Q=7, points=15)
    def test_lru_eviction(self) -> int | TestError:
        cache_server = [c for c in self.containers if c["Name"].endswith("-cache-3")]
        if len(cache_server) != 1:
            return TestError(
                message=f"Expected to find 1 container named *-cache-1 but found {cache_server}.",
                earned=0,
            )
        server_port = cache_server[0]["Publishers"][0]["PublishedPort"]
        addr = f"http://localhost:{server_port}"
        workload = [
            53704,
            53703,
            53719,  # misses
            53704,
            53703,
            53719,
            53711,  # first 3 should hit, then a miss
            53704,
            53703,
            53719,  # all misses
            53704,
            53705,  # hit, then a miss
            53704,  # if LRU, a hit, because 53704 wasn't evicted
        ]
        expected = [0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1]
        hits = []
        for zipcode in workload:
            url = f"{addr}/lookup/{zipcode}?limit=8"
            r = requests.get(url)
            r.raise_for_status()
            result = r.json()
            hits.append(int(result["source"] == "cache"))

        if hits != expected:
            return TestError(
                message=f"For workload, expected hits {expected} but got {hits}.",
                earned=0,
            )
        return 15

    def _kill_dataset(self, server_suffix: str) -> str | TestError:
        dataset_server = [
            c for c in self.containers if c["Name"].endswith(server_suffix)
        ]
        if len(dataset_server) != 1:
            return TestError(
                message=f"Expected to find 1 container named *-dataset-1 but found {dataset_server}.",
                earned=0,
            )
        try:
            output = subprocess.run(
                ["bash", "-c", f"docker kill {dataset_server[0]['Name']}"],
                capture_output=True,
                timeout=60 * 5,
            )
        except subprocess.TimeoutExpired:
            return TestError(message="`docker kill` timed out.", earned=0)
        if output.returncode != 0:
            return TestError(
                message=f"Failed to run docker kill: {output.stderr}", earned=0
            )

        cache_server = [c for c in self.containers if c["Name"].endswith("-cache-1")]
        if len(cache_server) != 1:
            return TestError(
                message=f"Expected to find 1 container named *-cache-1 but found {cache_server}.",
                earned=0,
            )
        server_port = cache_server[0]["Publishers"][0]["PublishedPort"]
        return f"http://localhost:{server_port}"

    @graded(Q=8, points=15)
    def test_one_dataset_down(self) -> int | TestError:
        # kill dataset server 1
        address = self._kill_dataset(server_suffix="-dataset-1")
        if isinstance(address, TestError):
            return address
        for i in range(6):
            url = f"{address}/lookup/{53706}?limit=32"
            r = requests.get(url)
            r.raise_for_status()
            result = r.json()
            if result["source"] != "2":
                return TestError(
                    message=f'When dataset server 1 is down, source should be "2".',
                    earned=0,
                )
        return 15

    @graded(Q=9, points=15)
    def test_two_datasets_down(self) -> int | TestError:
        # kill dataset server 2
        address = self._kill_dataset(server_suffix="-dataset-2")
        if isinstance(address, TestError):
            return address
        # this should be in the cache (from previous test),
        # so it shouldn't matter the datasets are down
        url = f"{address}/lookup/{53706}?limit=8"
        r = requests.get(url)
        r.raise_for_status()
        result = r.json()
        if result["source"] != "cache":
            return TestError(
                message=f'Expected "cache" source when data was cached, even if all dataset servers are down.',
                earned=0,
            )
        # this should fail because it is large and should go to a
        # dataset service, but all are down
        url = f"{address}/lookup/{53706}?limit=32"
        r = requests.get(url)
        r.raise_for_status()
        result = r.json()
        if not result.get("error"):
            return TestError(
                message=f"Both dataset servers are down, "
                f"so the cache server should return an error, "
                f"but we got back: {result}.",
                earned=0,
            )
        return 15


@register(project=Project.P2, registry=Registry.CALLBACK)
class ProjectTwoCallback(Callback):
    def on_setup(self):
        self.on_teardown()  # in case it didn't clean up well last time

    def on_teardown(self):
        subprocess.run(
            DockerComposeTargetWrapper(f="docker-compose.yml", subcommand="kill"),
            env={"PROJECT": self.delegate.grade_id, **dict(os.environ)},
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            f"docker rmi {self.delegate.grade_id}-dataset".split(),
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            f"docker rmi {self.delegate.grade_id}-cache".split(),
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )

    def on_validate_required_resources(self):
        """
        :throws MissingRequiredResource:
        :return:
        """
        required = [
            IOTargetWrapper("Dockerfile.cache"),
            IOTargetWrapper("Dockerfile.dataset"),
        ]
        for file in required:
            if not os.path.exists(file):
                raise MissingRequiredResource(required)

    def on_before_test(self):
        pass

    def on_after_test(self):
        pass