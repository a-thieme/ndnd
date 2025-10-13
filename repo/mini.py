from time import sleep
from mininet.log import setLogLevel, info

from minindn.minindn import Minindn
from minindn.util import MiniNDNCLI
from minindn.apps.app_manager import AppManager
from minindn.apps.nfd import Nfd
from minindn.apps.nlsr import Nlsr
from minindn.helpers.ndn_routing_helper import NdnRoutingHelper

if __name__ == "__main__":
    setLogLevel("info")

    Minindn.cleanUp()
    Minindn.verifyDependencies()
    ndn = Minindn()
    ndn.start()

    info("Starting NFD on nodes\n")
    nfds = AppManager(ndn, ndn.net.hosts, Nfd)
    info("Starting NLSR on nodes\n")
    nlsrs = AppManager(ndn, ndn.net.hosts, Nlsr)
    sleep(10)

    # using local testbed.conf topology, taken from named-data.github.io/testbed/ on 10/12/2025
    # arizona delft frankfurt memphis minho mml1 mml2 savi singapore srru tno ucla ufba urjc waseda wu
    repo1 = ndn.net["ucla"]
    repo2 = ndn.net["arizona"]
    repo3 = ndn.net["memphis"]
    repo4 = ndn.net["tno"]
    repo5 = ndn.net["wu"]
    repos = [repo1, repo2, repo3, repo4, repo5]

    producer = ndn.net["mml1"]
    # nodes:

    info("Adding static routes to NFD\n")
    grh = NdnRoutingHelper(ndn.net, "udp", "link-state")
    # add repo nodes address
    for i, repo in enumerate(repos):
        grh.addOrigin(
            [repo],
            [
                "/ndn/repo",
                # f"/ndn/node{i + 1}",
                # f"/ndn/node{i + 1}/notify",
                # f"/ndn/node{i + 1}/status",
            ],
        )
    # add producer address
    grh.addOrigin([producer], ["/test/producer/1", "/test"])
    grh.calculateNPossibleRoutes()

    info("Starting Repo on nodes\n")
    for i, host in enumerate(ndn.net.hosts):
        host.cmd(
            "nfdc strategy set /ndn/repo/awareness /localhost/nfd/strategy/multicast"
        )
        host.cmd(
            "nfdc strategy set /ndn/repo/heartbeat /localhost/nfd/strategy/multicast"
        )
        host.cmd(
            "nfdc strategy set /ndn/repo/commands /localhost/nfd/strategy/multicast"
        )

    for i, repo in enumerate(repos):
        repo.cmd(f"echo hi > /logs/echo{i}.log")
        repo.cmd(
            f"/repo/running/bin/repo /repo/running/repo_test_{i + 1}.yml /repo/running/repo_group_1.yml &> /repo/running/logs/repo_{i + 1}.log &"
        )

    sleep(10)
    producer.cmd(
        "/repo/running/bin/producer /ndn/repo /test/producer/1 &> /repo/running/logs/producer &"
    )
    MiniNDNCLI(ndn.net)
    ndn.stop()
