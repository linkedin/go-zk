package zk

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
)

type hostPort struct {
	host, port string
}

func (hp *hostPort) String() string {
	return hp.host + ":" + hp.port
}

// DNSHostProvider is the default HostProvider. It currently supports two modes: dynamic or static
// resolution of the addresses backing the ZK hosts.
//
// When in dynamic resolution mode, it will iterate through the ZK hosts on every call to Next, and
// return a random address selected from the resolved addresses of the ZK host (if the host is
// already an IP, it will return that directly). It is important to manually resolve and shuffle the
// addresses because the DNS record that backs a host may rarely (or never) change, so repeated calls
// to connect to this host may always connect to the same IP. This mode is the default mode, and
// matches the Java client's implementation. Note that if the host cannot be resolved, Next will
// return it directly, instead of an error. This will cause Dial to fail and the loop will move on to
// a new host.
// https://github.com/linkedin/zookeeper/blob/629518b5ea2b26d88a9ec53d5a422afe9b12e452/zookeeper-server/src/main/java/org/apache/zookeeper/client/StaticHostProvider.java#L368
//
// When in static resolution mode, it resolves the hosts once during Init, and iterates through the
// resolved addresses for every call to Next. Note that if the addresses that back the ZK hosts
// change, those changes will not be reflected. This mode is preserved to support previous behavior,
// and is not the default.
type DNSHostProvider struct {
	static bool

	mu         sync.Mutex // Protects everything, so we can add asynchronous updates later.
	servers    []hostPort
	curr       int
	last       int
	lookupHost func(string) ([]string, error) // Override of net.LookupHost, for testing.
}

func NewDynamicDNSHostProvider() HostProvider {
	return &DNSHostProvider{static: false}
}

func NewStaticDNSHostProvider() HostProvider {
	return &DNSHostProvider{static: true}
}

func (hp *DNSHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	lookupHost := hp.lookupHost
	if lookupHost == nil {
		lookupHost = net.LookupHost
	}

	var found []hostPort
	for _, server := range servers {
		host, port, err := net.SplitHostPort(server)
		if err != nil {
			return err
		}
		addrs, err := lookupHost(host)
		if err != nil {
			return err
		}

		if hp.static {
			for _, addr := range addrs {
				found = append(found, hostPort{addr, port})
			}
		} else {
			// In dynamic mode, the lookup is still performed to validate that the given ZK servers currently
			// resolve, however these results are discarded as the addresses will be resolved dynamically when
			// Next is called.
			found = append(found, hostPort{host, port})
		}
	}

	if len(found) == 0 {
		return fmt.Errorf("zk: no hosts found for addresses %q", servers)
	}

	// Randomize the order of the servers to avoid creating hotspots
	rand.Shuffle(len(found), func(i, j int) { found[i], found[j] = found[j], found[i] })

	hp.servers = found
	hp.last = len(hp.servers) - 1

	return nil
}

// Next returns the next server to connect to. retryStart should be true if this call to Next
// exhausted the list of known servers without Connected being called. If connecting to this final
// host fails, the connect loop will back off before invoking Next again for a fresh server.
func (hp *DNSHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	retryStart = hp.curr == hp.last

	next := hp.servers[hp.curr]
	if hp.static {
		server = next.String()
	} else {
		addrs, err := hp.lookupHost(next.host)
		if len(addrs) == 0 {
			if err == nil {
				// If for whatever reason lookupHosts returned an empty list of addresses but a nil error, use a
				// default error
				err = fmt.Errorf("zk: no hosts resolved by lookup for %q", next.host)
			}
			slog.Warn("Could not resolve ZK host", "host", next.host, "err", err)
			//
			server = next.String()
		} else {
			server = addrs[rand.Intn(len(addrs))] + ":" + next.port
		}
	}

	hp.curr = (hp.curr + 1) % len(hp.servers)

	return server, retryStart
}

// Connected notifies the HostProvider of a successful connection.
func (hp *DNSHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if hp.curr == 0 {
		hp.last = len(hp.servers) - 1
	} else {
		hp.last = hp.curr - 1
	}
}
