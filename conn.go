// Package zk is a native Go client library for the ZooKeeper orchestration service.
package zk

/*
TODO:
* make sure a ping response comes back in a reasonable time

Possible watcher events:
* Event{Type: EventNotWatching, State: StateDisconnected, Path: path, Err: err}
*/

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoServer indicates that an operation cannot be completed
// because attempts to connect to all servers in the list failed.
var ErrNoServer = errors.New("zk: could not connect to a server")

// ErrInvalidPath indicates that an operation was being attempted on
// an invalid path. (e.g. empty path).
var ErrInvalidPath = errors.New("zk: invalid path")

const (
	bufferSize      = 1536 * 1024
	eventChanSize   = 6
	sendChanSize    = 16
	protectedPrefix = "_c_"
)

type watchType int

const (
	watchTypeData watchType = iota
	watchTypeExist
	watchTypeChild
	watchTypePersistent
	watchTypePersistentRecursive
)

func (w watchType) isPersistent() bool {
	return w == watchTypePersistent || w == watchTypePersistentRecursive
}

type watchPathType struct {
	path  string
	wType watchType
}

type authCreds struct {
	scheme string
	auth   []byte
}

// Conn is the client connection and tracks all details for communication with the server.
type Conn struct {
	lastZxid       int64
	sessionID      int64
	state          State // must be 32-bit aligned
	xid            uint32
	sessionTimeout time.Duration
	passwd         []byte

	dialer         Dialer
	hostProvider   HostProvider
	serverMu       sync.Mutex // protects server
	server         string     // remember the address/port of the current server
	conn           net.Conn
	eventChan      chan Event
	eventCallback  EventCallback // may be nil
	shouldQuit     chan struct{}
	shouldQuitOnce sync.Once
	pingInterval   time.Duration
	recvTimeout    time.Duration
	connectTimeout time.Duration
	maxBufferSize  int
	metricReceiver MetricReceiver

	creds   []authCreds
	credsMu sync.Mutex // protects server

	sendChan     chan *request
	requests     map[int32]*request // Xid -> pending request
	requestsLock sync.Mutex
	watchers     map[watchPathType][]EventQueue
	watchersLock sync.Mutex
	closeChan    chan struct{} // channel to tell send loop stop

	// Debug (used by unit tests)
	reconnectLatch   chan struct{}
	setWatchLimit    int
	setWatchCallback func([]*setWatchesRequest)

	// Debug (for recurring re-auth hang)
	debugCloseRecvLoop bool
	resendZkAuthFn     func(context.Context, *Conn) error

	buf []byte
}

// ConnOption represents a connection option.
type ConnOption interface {
	apply(*Conn)
}

type connOption func(*Conn)

func (c connOption) apply(conn *Conn) {
	c(conn)
}

type request struct {
	xid        int32
	opcode     int32
	pkt        interface{}
	recvStruct interface{}
	recvChan   chan response

	// Because sending and receiving happen in separate go routines, there's
	// a possible race condition when creating watches from outside the read
	// loop. We must ensure that a watcher gets added to the list synchronously
	// with the response from the server on any request that creates a watch.
	// In order to not hard code the watch logic for each opcode in the recv
	// loop the caller can use recvFunc to insert some synchronously code
	// after a response.
	recvFunc func(*request, *responseHeader, error)
}

type response struct {
	zxid int64
	err  error
}

// Event is an Znode event sent by the server.
// Refer to EventType for more details.
type Event struct {
	Type   EventType
	State  State
	Path   string // For non-session events, the path of the watched node.
	Err    error
	Server string // For connection events
	// For watch events, the zxid that caused the change (starting with ZK 3.9.0). For ping events, the zxid that the
	// server last processed. Note that the last processed zxid is only updated once the watch events have been
	// triggered. Since ZK operates over one connection, the watch events are therefore queued up before the ping. This
	// means watch events should always be received before pings, and receiving a ping with a given zxid means any watch
	// event for a lower zxid have already been received (if any).
	Zxid int64
	// This is the time at which the event was received by the client. Useful to understand lag across the entire
	// system. Note that this is NOT the time at which the event fired in the quorum. Only set for watch events and
	// pings.
	Timestamp time.Time
}

// HostProvider is used to represent a set of hosts a ZooKeeper client should connect to.
// It is an analog of the Java equivalent:
// http://svn.apache.org/viewvc/zookeeper/trunk/src/java/main/org/apache/zookeeper/client/HostProvider.java?view=markup
type HostProvider interface {
	// Init is called first, with the servers specified in the connection string.
	Init(servers []string) error
	// Next returns the next server to connect to. retryStart should be true if this call to Next
	// exhausted the list of known servers without Connected being called. If connecting to this final
	// host fails, the connect loop will back off before invoking Next again for a fresh server.
	Next() (server string, retryStart bool)
	// Connected notifies the HostProvider of a successful connection.
	Connected()
}

// Connect establishes a new connection to a pool of zookeeper
// servers. The provided session timeout sets the amount of time for which
// a session is considered valid after losing connection to a server. Within
// the session timeout it's possible to reestablish a connection to a different
// server and keep the same session. This is means any ephemeral nodes and
// watches are maintained.
func Connect(servers []string, sessionTimeout time.Duration, options ...ConnOption) (*Conn, <-chan Event, error) {
	if len(servers) == 0 {
		return nil, nil, errors.New("zk: server list must not be empty")
	}

	srvs := FormatServers(servers)

	// Randomize the order of the servers to avoid creating hotspots
	shuffleSlice(srvs)

	ec := make(chan Event, eventChanSize)
	conn := &Conn{
		dialer:         new(net.Dialer),
		hostProvider:   new(StaticHostProvider),
		conn:           nil,
		state:          StateDisconnected,
		eventChan:      ec,
		shouldQuit:     make(chan struct{}),
		connectTimeout: 1 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		requests:       make(map[int32]*request),
		watchers:       make(map[watchPathType][]EventQueue),
		passwd:         emptyPassword,
		buf:            make([]byte, bufferSize),
		resendZkAuthFn: resendZkAuth,
		metricReceiver: UnimplementedMetricReceiver{},
	}

	// Set provided options.
	for _, option := range options {
		option.apply(conn)
	}

	if err := conn.hostProvider.Init(srvs); err != nil {
		return nil, nil, err
	}

	conn.setTimeouts(sessionTimeout)
	// TODO: This context should be passed in by the caller to be the connection lifecycle context.
	ctx := context.Background()

	go func() {
		conn.loop(ctx)
		conn.flushRequests(ErrClosing)
		conn.invalidateWatches(ErrClosing)
		close(conn.eventChan)
	}()
	return conn, ec, nil
}

// Dialer is an interface implemented by the standard [net.Dialer] but also by [crypto/tls.Dialer].
type Dialer interface {
	// DialContext will be invoked when connecting to a ZK host.
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// WithDialer returns a connection option specifying a non-default Dialer. This can be used, for
// example, to enable TLS when connecting to the ZK server by passing in a [crypto/tls.Dialer].
func WithDialer(dialer Dialer) ConnOption {
	return connOption(func(c *Conn) {
		c.dialer = dialer
	})
}

// WithHostProvider returns a connection option specifying a non-default HostProvider.
func WithHostProvider(hostProvider HostProvider) ConnOption {
	return connOption(func(c *Conn) {
		c.hostProvider = hostProvider
	})
}

// EventCallback is a function that is called when an Event occurs.
type EventCallback func(Event)

// WithEventCallback returns a connection option that specifies an event
// callback.
// The callback must not block - doing so would delay the ZK go routines.
func WithEventCallback(cb EventCallback) ConnOption {
	return connOption(func(c *Conn) {
		c.eventCallback = cb
	})
}

// WithMaxBufferSize sets the maximum buffer size used to read and decode
// packets received from the Zookeeper server. The standard Zookeeper client for
// Java defaults to a limit of 1mb. For backwards compatibility, this Go client
// defaults to unbounded unless overridden via this option. A value that is zero
// or negative indicates that no limit is enforced.
//
// This is meant to prevent resource exhaustion in the face of potentially
// malicious data in ZK. It should generally match the server setting (which
// also defaults ot 1mb) so that clients and servers agree on the limits for
// things like the size of data in an individual znode and the total size of a
// transaction.
//
// For production systems, this should be set to a reasonable value (ideally
// that matches the server configuration). For ops tooling, it is handy to use a
// much larger limit, in order to do things like clean-up problematic state in
// the ZK tree. For example, if a single znode has a huge number of children, it
// is possible for the response to a "list children" operation to exceed this
// buffer size and cause errors in clients. The only way to subsequently clean
// up the tree (by removing superfluous children) is to use a client configured
// with a larger buffer size that can successfully query for all of the child
// names and then remove them. (Note there are other tools that can list all of
// the child names without an increased buffer size in the client, but they work
// by inspecting the servers' transaction logs to enumerate children instead of
// sending an online request to a server.
func WithMaxBufferSize(maxBufferSize int) ConnOption {
	return connOption(func(c *Conn) {
		c.maxBufferSize = maxBufferSize
	})
}

// WithMaxConnBufferSize sets maximum buffer size used to send and encode
// packets to Zookeeper server. The standard Zookeeper client for java defaults
// to a limit of 1mb. This option should be used for non-standard server setup
// where znode is bigger than default 1mb.
func WithMaxConnBufferSize(maxBufferSize int) ConnOption {
	return connOption(func(c *Conn) {
		c.buf = make([]byte, maxBufferSize)
	})
}

func WithMetricReceiver(mr MetricReceiver) ConnOption {
	return connOption(func(c *Conn) {
		c.metricReceiver = mr
	})
}

// Close will submit a close request with ZK and signal the connection to stop
// sending and receiving packets.
func (c *Conn) Close() {
	c.shouldQuitOnce.Do(func() {
		close(c.shouldQuit)

		select {
		case <-c.queueRequest(opClose, &closeRequest{}, &closeResponse{}, nil):
		case <-time.After(time.Second):
		}
	})
}

// State returns the current state of the connection.
func (c *Conn) State() State {
	return State(atomic.LoadInt32((*int32)(&c.state)))
}

// SessionID returns the current session id of the connection.
func (c *Conn) SessionID() int64 {
	return atomic.LoadInt64(&c.sessionID)
}

func (c *Conn) setTimeouts(sessionTimeout time.Duration) {
	c.sessionTimeout = sessionTimeout
	c.recvTimeout = sessionTimeout * 2 / 3
	c.pingInterval = c.recvTimeout / 2
}

func (c *Conn) setState(state State) {
	atomic.StoreInt32((*int32)(&c.state), int32(state))
	c.sendEvent(Event{Type: EventSession, State: state, Server: c.Server()})
}

func (c *Conn) sendEvent(evt Event) {
	if c.eventCallback != nil {
		c.eventCallback(evt)
	}

	select {
	case c.eventChan <- evt:
	default:
		// panic("zk: event channel full - it must be monitored and never allowed to be full")
	}
}

func (c *Conn) connect() (err error) {
	var retryStart bool
	for {
		c.serverMu.Lock()
		c.server, retryStart = c.hostProvider.Next()
		c.serverMu.Unlock()

		c.setState(StateConnecting)

		dial := func() (net.Conn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), c.connectTimeout)
			defer cancel()
			return c.dialer.DialContext(ctx, "tcp", c.Server())
		}

		slog.Info("Dialing ZK server", "server", c.Server())
		zkConn, err := dial()
		if err == nil {
			c.conn = zkConn
			c.setState(StateConnected)
			slog.Info("Connection established", "server", c.Server(), "addr", zkConn.RemoteAddr())
			return nil
		}

		slog.Warn("Failed to connect to ZK server", "server", c.Server(), "err", err)

		if retryStart {
			c.flushUnsentRequests(ErrNoServer)
			select {
			case <-time.After(time.Second):
				// pass
			case <-c.shouldQuit:
				c.setState(StateDisconnected)
				c.flushUnsentRequests(ErrClosing)
				return ErrClosing
			}
		}
	}
}

func (c *Conn) sendRequest(
	opcode int32,
	req interface{},
	res interface{},
	recvFunc func(*request, *responseHeader, error),
) (
	<-chan response,
	error,
) {
	rq := &request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 1),
		recvFunc:   recvFunc,
	}

	if err := c.sendData(rq); err != nil {
		return nil, err
	}

	return rq.recvChan, nil
}

func (c *Conn) loop(ctx context.Context) {
	for {
		if err := c.connect(); err != nil {
			// c.Close() was called
			return
		}

		var sendLoopErr, recvLoopErr error

		authErr := c.authenticate()
		switch {
		case errors.Is(authErr, ErrSessionExpired):
			slog.Warn("authentication failed", "err", authErr)
			c.invalidateWatches(authErr)
		case authErr != nil && c.conn != nil:
			slog.Warn("authentication failed", "err", authErr)
			c.conn.Close()
		case authErr == nil:
			slog.Info("authenticated", "sessionId", c.SessionID(), "timeout", c.sessionTimeout)
			c.hostProvider.Connected()        // mark success
			c.closeChan = make(chan struct{}) // channel to tell send loop stop

			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer c.conn.Close() // causes recv loop to EOF/exit
				defer wg.Done()

				if sendLoopErr = c.resendZkAuthFn(ctx, c); sendLoopErr != nil {
					slog.Warn("error in resending auth creds", "err", sendLoopErr)
					return
				}

				if sendLoopErr = c.sendLoop(); sendLoopErr != nil {
					slog.Warn("Send loop terminated with error", "err", sendLoopErr)
				} else {
					slog.Info("Send loop terminated")
				}
			}()

			wg.Add(1)
			go func() {
				defer close(c.closeChan) // tell send loop to exit
				defer wg.Done()

				if c.debugCloseRecvLoop {
					recvLoopErr = errors.New("DEBUG: close recv loop")
				} else {
					recvLoopErr = c.recvLoop(c.conn)
				}

				switch {
				case errors.Is(recvLoopErr, io.EOF):
					slog.Info("recv loop terminated")

				}
				if recvLoopErr != io.EOF {
					slog.Warn("recv loop terminated with error", "err", recvLoopErr)
				} else {
				}
				if recvLoopErr == nil {
					panic("zk: recvLoop should never return nil error")
				}
			}()

			c.sendSetWatches()
			wg.Wait()
		}

		c.setState(StateDisconnected)

		select {
		case <-c.shouldQuit:
			c.flushRequests(ErrClosing)
			return
		default:
		}

		// Surface an error that contains all errors that could have caused the connection to get closed
		err := errors.Join(authErr, sendLoopErr, recvLoopErr)
		if !errors.Is(err, ErrSessionExpired) || errors.Is(err, ErrConnectionClosed) {
			// Always default to ErrConnectionClosed for any error that doesn't already have it or
			// ErrSessionExpired as a cause, makes error handling more straightforward. Note that by definition,
			// reaching this point in the code means the connection was closed, hence using it as the default
			// value.
			err = errors.Join(ErrConnectionClosed, err)
		}
		c.flushRequests(err)

		if c.reconnectLatch != nil {
			select {
			case <-c.shouldQuit:
				return
			case <-c.reconnectLatch:
			}
		}
	}
}

func (c *Conn) flushUnsentRequests(err error) {
	for {
		select {
		default:
			return
		case req := <-c.sendChan:
			req.recvChan <- response{-1, err}
		}
	}
}

// Send error to all pending requests and clear request map
func (c *Conn) flushRequests(err error) {
	c.requestsLock.Lock()
	for _, req := range c.requests {
		req.recvChan <- response{-1, err}
	}
	c.requests = make(map[int32]*request)
	c.requestsLock.Unlock()
}

var eventWatchTypes = map[EventType][]watchType{
	EventNodeCreated:         {watchTypeExist, watchTypePersistent, watchTypePersistentRecursive},
	EventNodeDataChanged:     {watchTypeExist, watchTypeData, watchTypePersistent, watchTypePersistentRecursive},
	EventNodeChildrenChanged: {watchTypeChild, watchTypePersistent},
	EventNodeDeleted:         {watchTypeExist, watchTypeData, watchTypeChild, watchTypePersistent, watchTypePersistentRecursive},
	EventPingReceived:        nil,
}
var persistentWatchTypes = []watchType{watchTypePersistent, watchTypePersistentRecursive}

// Send event to all interested watchers
func (c *Conn) notifyWatches(ev Event) {
	wTypes, ok := eventWatchTypes[ev.Type]
	if !ok {
		return
	}

	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if ev.Type == EventPingReceived {
		for wpt, watchers := range c.watchers {
			if wpt.wType.isPersistent() {
				for _, ch := range watchers {
					ch.Push(ev)
				}
			}
		}
	} else {
		broadcast := func(wpt watchPathType) {
			for _, ch := range c.watchers[wpt] {
				ch.Push(ev)
				if !wpt.wType.isPersistent() {
					ch.Close()
					delete(c.watchers, wpt)
				}
			}
		}

		for _, t := range wTypes {
			if t == watchTypePersistentRecursive {
				for p := ev.Path; ; p, _ = SplitPath(p) {
					broadcast(watchPathType{p, t})
					if p == "/" {
						break
					}
				}
			} else {
				broadcast(watchPathType{ev.Path, t})
			}
		}
	}
}

// Send error to all watchers and clear watchers map
func (c *Conn) invalidateWatches(err error) {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.watchers) > 0 {
		for pathType, watchers := range c.watchers {
			if errors.Is(err, ErrSessionExpired) && pathType.wType.isPersistent() {
				// Ignore ErrSessionExpired for persistent watchers as the client will either automatically reconnect,
				// or this is a shutdown-worthy error in which case there will be a followup invocation of this method
				// with ErrClosing
				continue
			}

			ev := Event{Type: EventNotWatching, State: StateDisconnected, Path: pathType.path, Err: err}
			c.sendEvent(ev) // also publish globally
			for _, ch := range watchers {
				ch.Push(ev)
				ch.Close()
			}
			delete(c.watchers, pathType)
		}
	}
}

func (c *Conn) sendSetWatches() {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	if len(c.watchers) == 0 {
		return
	}

	slog.Info("Resetting watches after reconnect", "watchCount", len(c.watchers))

	// NB: A ZK server, by default, rejects packets >1mb. So, if we have too
	// many watches to reset, we need to break this up into multiple packets
	// to avoid hitting that limit. Mirroring the Java client behavior: we are
	// conservative in that we limit requests to 128kb (since server limit is
	// is actually configurable and could conceivably be configured smaller
	// than default of 1mb).
	limit := 128 * 1024
	if c.setWatchLimit > 0 {
		limit = c.setWatchLimit
	}

	var reqs []*setWatchesRequest
	var req *setWatchesRequest
	var sizeSoFar int

	n := 0
	for pathType, watchers := range c.watchers {
		if len(watchers) == 0 {
			continue
		}
		addlLen := 4 + len(pathType.path)
		if req == nil || sizeSoFar+addlLen > limit {
			if req != nil {
				// add to set of requests that we'll send
				reqs = append(reqs, req)
			}
			sizeSoFar = 28 // fixed overhead of a set-watches packet
			req = &setWatchesRequest{RelativeZxid: c.lastZxid}
		}
		sizeSoFar += addlLen
		switch pathType.wType {
		case watchTypeData:
			req.DataWatches = append(req.DataWatches, pathType.path)
		case watchTypeExist:
			req.ExistWatches = append(req.ExistWatches, pathType.path)
		case watchTypeChild:
			req.ChildWatches = append(req.ChildWatches, pathType.path)
		case watchTypePersistent:
			req.PersistentWatches = append(req.PersistentWatches, pathType.path)
		case watchTypePersistentRecursive:
			req.PersistentRecursiveWatches = append(req.PersistentRecursiveWatches, pathType.path)
		}
		n++
	}
	if n == 0 {
		return
	}
	if req != nil { // don't forget any trailing packet we were building
		reqs = append(reqs, req)
	}

	if c.setWatchCallback != nil {
		c.setWatchCallback(reqs)
	}

	go func() {
		res := &setWatchesResponse{}
		// TODO: Pipeline these so queue all of them up before waiting on any
		// response. That will require some investigation to make sure there
		// aren't failure modes where a blocking write to the channel of requests
		// could hang indefinitely and cause this goroutine to leak...
		for _, req := range reqs {
			var op int32 = opSetWatches
			if len(req.PersistentWatches) > 0 || len(req.PersistentRecursiveWatches) > 0 {
				// to maintain compatibility with older servers, only send opSetWatches2 if persistent watches are used
				op = opSetWatches2
			}

			_, err := c.request(op, req, res, func(r *request, header *responseHeader, err error) {
				if err == nil && op == opSetWatches2 {
					// If the setWatches was successful, notify the persistent watchers they've been reconnected.
					// Because we process responses in one routine, we know that the following will execute before
					// subsequent responses are processed. This means we won't end up in a situation where events are
					// sent to watchers before the reconnect event is sent.
					c.watchersLock.Lock()
					defer c.watchersLock.Unlock()
					for _, wt := range persistentWatchTypes {
						var paths []string
						if wt == watchTypePersistent {
							paths = req.PersistentWatches
						} else {
							paths = req.PersistentRecursiveWatches
						}
						for _, p := range paths {
							e := Event{Type: EventWatching, State: StateConnected, Path: p}
							c.sendEvent(e) // also publish globally
							for _, ch := range c.watchers[watchPathType{path: p, wType: wt}] {
								ch.Push(e)
							}
						}
					}
				}
			})
			if err != nil {
				slog.Warn("Failed to set previous watches", "err", err)
				break
			}
		}
	}()
}

func (c *Conn) authenticate() error {
	buf := make([]byte, 256)

	// Encode and send a connect request.
	n, err := encodePacket(buf[4:], &connectRequest{
		ProtocolVersion: protocolVersion,
		LastZxidSeen:    c.lastZxid,
		// The timeout in the connect request is milliseconds
		TimeOut:   int32(c.sessionTimeout / time.Millisecond),
		SessionID: c.SessionID(),
		Passwd:    c.passwd,
	})
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout * 10))
	_, err = c.conn.Write(buf[:n+4])
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return err
	}

	// Receive and decode a connect response.
	c.conn.SetReadDeadline(time.Now().Add(c.recvTimeout * 10))
	_, err = io.ReadFull(c.conn, buf[:4])
	c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}

	blen := int(binary.BigEndian.Uint32(buf[:4]))
	if cap(buf) < blen {
		buf = make([]byte, blen)
	}

	_, err = io.ReadFull(c.conn, buf[:blen])
	if err != nil {
		return err
	}

	r := connectResponse{}
	_, err = decodePacket(buf[:blen], &r)
	if err != nil {
		return err
	}
	if r.SessionID == 0 {
		atomic.StoreInt64(&c.sessionID, int64(0))
		c.passwd = emptyPassword
		c.lastZxid = 0
		c.setState(StateExpired)
		return ErrSessionExpired
	}

	atomic.StoreInt64(&c.sessionID, r.SessionID)
	c.setTimeouts(time.Duration(r.TimeOut) * time.Millisecond)
	c.passwd = r.Passwd
	c.setState(StateHasSession)

	return nil
}

func (c *Conn) sendData(req *request) error {
	header := &requestHeader{req.xid, req.opcode}
	n, err := encodePacket(c.buf[4:], header)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	n2, err := encodePacket(c.buf[4+n:], req.pkt)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	n += n2

	binary.BigEndian.PutUint32(c.buf[:4], uint32(n))

	c.requestsLock.Lock()
	select {
	case <-c.closeChan:
		req.recvChan <- response{-1, ErrConnectionClosed}
		c.requestsLock.Unlock()
		return ErrConnectionClosed
	default:
	}
	c.requests[req.xid] = req
	c.requestsLock.Unlock()

	c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout))
	_, err = c.conn.Write(c.buf[:n+4])
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		req.recvChan <- response{-1, err}
		c.conn.Close()
		return err
	}

	return nil
}

func (c *Conn) sendLoop() error {
	pingTicker := time.NewTicker(c.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case req := <-c.sendChan:
			if err := c.sendData(req); err != nil {
				return err
			}
		case <-pingTicker.C:
			n, err := encodePacket(c.buf[4:], &requestHeader{Xid: -2, Opcode: opPing})
			if err != nil {
				panic("zk: opPing should never fail to serialize")
			}

			binary.BigEndian.PutUint32(c.buf[:4], uint32(n))

			c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout))
			_, err = c.conn.Write(c.buf[:n+4])
			c.conn.SetWriteDeadline(time.Time{})
			if err != nil {
				c.conn.Close()
				return err
			}
			c.metricReceiver.PingSent()
		case <-c.closeChan:
			return nil
		}
	}
}

func (c *Conn) recvLoop(conn net.Conn) error {
	sz := bufferSize
	if c.maxBufferSize > 0 && sz > c.maxBufferSize {
		sz = c.maxBufferSize
	}
	buf := make([]byte, sz)
	for {
		// package length
		if err := conn.SetReadDeadline(time.Now().Add(c.recvTimeout)); err != nil {
			slog.Warn("failed to set connection deadline", "err", err)
		}
		_, err := io.ReadFull(conn, buf[:4])
		if err != nil {
			return fmt.Errorf("failed to read from connection: %w", err)
		}

		blen := int(binary.BigEndian.Uint32(buf[:4]))
		if cap(buf) < blen {
			if c.maxBufferSize > 0 && blen > c.maxBufferSize {
				return fmt.Errorf(
					"%w (packet size %d, max buffer size: %d)",
					ErrResponseBufferSizeExceeded, blen, c.maxBufferSize,
				)
			}
			buf = make([]byte, blen)
		}

		_, err = io.ReadFull(conn, buf[:blen])
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			return err
		}

		res := responseHeader{}
		_, err = decodePacket(buf[:16], &res)
		if err != nil {
			return err
		}

		if res.Xid == -1 {
			we := &watcherEvent{}
			_, err = decodePacket(buf[16:blen], we)
			if err != nil {
				return err
			}
			ev := Event{
				Type:      we.Type,
				State:     we.State,
				Path:      we.Path,
				Err:       nil,
				Timestamp: time.Now(),
			}
			c.sendEvent(ev)
			c.notifyWatches(ev)
		} else if res.Xid == -2 {
			// Ping response. Ignore.
			c.metricReceiver.PongReceived()
			c.notifyWatches(Event{
				Type:      EventPingReceived,
				State:     StateHasSession,
				Zxid:      res.Zxid,
				Timestamp: time.Now(),
			})
		} else if res.Xid < 0 {
			slog.Warn("Xid < 0 but not ping or watcher event", "Xid", res.Xid)
		} else {
			if res.Zxid > 0 {
				c.lastZxid = res.Zxid
			}

			c.requestsLock.Lock()
			req, ok := c.requests[res.Xid]
			if ok {
				delete(c.requests, res.Xid)
			}
			c.requestsLock.Unlock()

			if !ok {
				slog.Warn("Ignoring response for unknown request", "xid", res.Xid)
			} else {
				if res.Err != 0 {
					err = res.Err.toError()
				} else {
					_, err = decodePacket(buf[16:blen], req.recvStruct)
				}
				if req.recvFunc != nil {
					req.recvFunc(req, &res, err)
				}
				req.recvChan <- response{res.Zxid, err}
				if req.opcode == opClose {
					return io.EOF
				}
			}
		}
	}
}

func (c *Conn) nextXid() int32 {
	return int32(atomic.AddUint32(&c.xid, 1) & 0x7fffffff)
}

func (c *Conn) addWatcher(path string, watchType watchType, ch EventQueue) {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	wpt := watchPathType{path, watchType}
	c.watchers[wpt] = append(c.watchers[wpt], ch)
	if watchType.isPersistent() {
		ch.Push(Event{Type: EventWatching, State: StateConnected, Path: path})
	}
}

func (c *Conn) queueRequest(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) <-chan response {
	rq := &request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 2),
		recvFunc:   recvFunc,
	}

	switch opcode {
	case opClose:
		// always attempt to send close ops.
		select {
		case c.sendChan <- rq:
		case <-time.After(c.connectTimeout * 2):
			slog.Warn("gave up trying to send opClose to server")
			rq.recvChan <- response{-1, ErrConnectionClosed}
		}
	default:
		// otherwise avoid deadlocks for dumb clients who aren't aware that
		// the ZK connection is closed yet.
		select {
		case <-c.shouldQuit:
			rq.recvChan <- response{-1, ErrConnectionClosed}
		case c.sendChan <- rq:
			// check for a tie
			select {
			case <-c.shouldQuit:
				// maybe the caller gets this, maybe not- we tried.
				rq.recvChan <- response{-1, ErrConnectionClosed}
			default:
			}
		}
	}
	return rq.recvChan
}

func (c *Conn) request(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) (_ int64, err error) {
	start := time.Now()
	defer func() {
		c.metricReceiver.RequestCompleted(time.Now().Sub(start), err)
	}()

	recv := c.queueRequest(opcode, req, res, recvFunc)
	select {
	case r := <-recv:
		return r.zxid, r.err
	case <-c.shouldQuit:
		// queueRequest() can be racy, double-check for the race here and avoid
		// a potential data-race. otherwise the client of this func may try to
		// access `res` fields concurrently w/ the async response processor.
		// NOTE: callers of this func should check for (at least) ErrConnectionClosed
		// and avoid accessing fields of the response object if such error is present.
		return -1, ErrConnectionClosed
	}
}

// AddAuth adds an authentication config to the connection.
func (c *Conn) AddAuth(scheme string, auth []byte) error {
	_, err := c.request(opSetAuth, &setAuthRequest{Type: 0, Scheme: scheme, Auth: auth}, &setAuthResponse{}, nil)

	if err != nil {
		return err
	}

	// Remember authdata so that it can be re-submitted on reconnect
	//
	// FIXME(prozlach): For now we treat "userfoo:passbar" and "userfoo:passbar2"
	// as two different entries, which will be re-submitted on reconnect. Some
	// research is needed on how ZK treats these cases and
	// then maybe switch to something like "map[username] = password" to allow
	// only single password for given user with users being unique.
	obj := authCreds{
		scheme: scheme,
		auth:   auth,
	}

	c.credsMu.Lock()
	c.creds = append(c.creds, obj)
	c.credsMu.Unlock()

	return nil
}

// Children returns the children of a znode.
func (c *Conn) Children(path string) ([]string, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getChildren2Response{}
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: false}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, nil, err
	}
	return res.Children, res.Stat, err
}

// ChildrenW returns the children of a znode and sets a watch.
func (c *Conn) ChildrenW(path string) ([]string, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech ChanQueue[Event]
	res := &getChildren2Response{}
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = newChanEventChannel()
			c.addWatcher(path, watchTypeChild, ech)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Children, res.Stat, ech, err
}

// Get gets the contents of a znode.
func (c *Conn) Get(path string) ([]byte, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: false}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, nil, err
	}
	return res.Data, res.Stat, err
}

// GetW returns the contents of a znode and sets a watch
func (c *Conn) GetW(path string) ([]byte, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech ChanQueue[Event]
	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = newChanEventChannel()
			c.addWatcher(path, watchTypeData, ech)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Data, res.Stat, ech, err
}

// Set updates the contents of a znode.
func (c *Conn) Set(path string, data []byte, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setDataResponse{}
	_, err := c.request(opSetData, &SetDataRequest{path, data, version}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, err
	}
	return res.Stat, err
}

// Create creates a znode. If acl is empty, it uses the global WorldACL with PermAll
// The returned path is the new path assigned by the server, it may not be the
// same as the input, for example when creating a sequence znode the returned path
// will be the input path with a sequence number appended.
func (c *Conn) Create(path string, data []byte, flags int32, acl []ACL) (string, error) {
	if err := validatePath(path, flags&FlagSequence == FlagSequence); err != nil {
		return "", err
	}

	if len(acl) == 0 {
		acl = WorldACL(PermAll)
	}

	res := &createResponse{}
	_, err := c.request(opCreate, &CreateRequest{path, data, acl, flags}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return "", err
	}
	return res.Path, err
}

// CreateAndReturnStat is the equivalent of Create, but it also returns the Stat of the created node.
func (c *Conn) CreateAndReturnStat(path string, data []byte, flags int32, acl []ACL) (string, *Stat, error) {
	if err := validatePath(path, flags&FlagSequence == FlagSequence); err != nil {
		return "", nil, err
	}

	if len(acl) == 0 {
		acl = WorldACL(PermAll)
	}

	res := &create2Response{}
	_, err := c.request(opCreate2, &CreateRequest{path, data, acl, flags}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return "", nil, err
	}
	return res.Path, res.Stat, err
}

// CreateContainer creates a container znode and returns the path.
func (c *Conn) CreateContainer(path string, data []byte, flags int32, acl []ACL) (string, error) {
	if err := validatePath(path, flags&FlagSequence == FlagSequence); err != nil {
		return "", err
	}
	if flags&FlagTTL != FlagTTL {
		return "", ErrInvalidFlags
	}

	res := &createResponse{}
	_, err := c.request(opCreateContainer, &CreateContainerRequest{path, data, acl, flags}, res, nil)
	return res.Path, err
}

// CreateTTL creates a TTL znode, which will be automatically deleted by server after the TTL.
func (c *Conn) CreateTTL(path string, data []byte, flags int32, acl []ACL, ttl time.Duration) (string, error) {
	if err := validatePath(path, flags&FlagSequence == FlagSequence); err != nil {
		return "", err
	}
	if flags&FlagTTL != FlagTTL {
		return "", ErrInvalidFlags
	}

	res := &createResponse{}
	_, err := c.request(opCreateTTL, &CreateTTLRequest{path, data, acl, flags, ttl.Milliseconds()}, res, nil)
	return res.Path, err
}

// CreateProtectedEphemeralSequential fixes a race condition if the server crashes
// after it creates the node. On reconnect the session may still be valid so the
// ephemeral node still exists. Therefore, on reconnect we need to check if a node
// with a GUID generated on create exists.
func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []ACL) (string, error) {
	if err := validatePath(path, true); err != nil {
		return "", err
	}

	var guid [16]byte
	_, err := io.ReadFull(rand.Reader, guid[:16])
	if err != nil {
		return "", err
	}
	guidStr := fmt.Sprintf("%x", guid)

	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s%s-%s", protectedPrefix, guidStr, parts[len(parts)-1])
	rootPath := strings.Join(parts[:len(parts)-1], "/")
	protectedPath := strings.Join(parts, "/")

	var newPath string
	for i := 0; i < 3; i++ {
		newPath, err = c.Create(protectedPath, data, FlagEphemeral|FlagSequence, acl)
		switch {
		case errors.Is(err, ErrSessionExpired):
			// No need to search for the node since it can't exist. Just try again.
		case errors.Is(err, ErrConnectionClosed):
			children, _, err := c.Children(rootPath)
			if err != nil {
				return "", err
			}
			for _, p := range children {
				parts := strings.Split(p, "/")
				if pth := parts[len(parts)-1]; strings.HasPrefix(pth, protectedPrefix) {
					if g := pth[len(protectedPrefix) : len(protectedPrefix)+32]; g == guidStr {
						return rootPath + "/" + p, nil
					}
				}
			}
		case err == nil:
			return newPath, nil
		default:
			return "", err
		}
	}
	return "", err
}

// Delete deletes a znode.
func (c *Conn) Delete(path string, version int32) error {
	if err := validatePath(path, false); err != nil {
		return err
	}

	_, err := c.request(opDelete, &DeleteRequest{path, version}, &deleteResponse{}, nil)
	return err
}

// Exists tells the existence of a znode.
func (c *Conn) Exists(path string) (bool, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return false, nil, err
	}

	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: false}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return false, nil, err
	}
	exists := true
	if errors.Is(err, ErrNoNode) {
		exists = false
		err = nil
	}
	return exists, res.Stat, err
}

// ExistsW tells the existence of a znode and sets a watch.
func (c *Conn) ExistsW(path string) (bool, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return false, nil, nil, err
	}

	var ech ChanQueue[Event]
	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		ech = newChanEventChannel()
		if err == nil {
			c.addWatcher(path, watchTypeData, ech)
		} else if errors.Is(err, ErrNoNode) {
			c.addWatcher(path, watchTypeExist, ech)
		}
	})
	exists := true
	if errors.Is(err, ErrNoNode) {
		exists = false
		err = nil
	}
	if err != nil {
		return false, nil, nil, err
	}
	return exists, res.Stat, ech, err
}

// GetACL gets the ACLs of a znode.
func (c *Conn) GetACL(path string) ([]ACL, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getAclResponse{}
	_, err := c.request(opGetAcl, &getAclRequest{Path: path}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, nil, err
	}
	return res.Acl, res.Stat, err
}

// SetACL updates the ACLs of a znode.
func (c *Conn) SetACL(path string, acl []ACL, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setAclResponse{}
	_, err := c.request(opSetAcl, &setAclRequest{Path: path, Acl: acl, Version: version}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, err
	}
	return res.Stat, err
}

// Sync flushes the channel between process and the leader of a given znode,
// you may need it if you want identical views of ZooKeeper data for 2 client instances.
// Please refer to the "Consistency Guarantees" section of ZK document for more details.
func (c *Conn) Sync(path string) (string, error) {
	if err := validatePath(path, false); err != nil {
		return "", err
	}

	res := &syncResponse{}
	_, err := c.request(opSync, &syncRequest{Path: path}, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return "", err
	}
	return res.Path, err
}

// MultiResponse is the result of a Multi call.
type MultiResponse struct {
	Stat   *Stat
	String string
	Error  error
}

// Multi executes multiple ZooKeeper operations or none of them. The provided
// ops must be one of *CreateRequest, *DeleteRequest, *SetDataRequest, or
// *CheckVersionRequest.
func (c *Conn) Multi(ops ...interface{}) ([]MultiResponse, error) {
	req := &multiRequest{
		Ops: make([]multiRequestOp, 0, len(ops)),
	}
	for _, op := range ops {
		var opCode int32
		switch op.(type) {
		case *CreateRequest:
			opCode = opCreate
		case *SetDataRequest:
			opCode = opSetData
		case *DeleteRequest:
			opCode = opDelete
		case *CheckVersionRequest:
			opCode = opCheck
		default:
			return nil, fmt.Errorf("unknown operation type %T", op)
		}
		req.Ops = append(req.Ops, multiRequestOp{multiHeader{opCode, false, -1}, op})
	}
	res := &multiResponse{}
	_, err := c.request(opMulti, req, res, nil)
	if errors.Is(err, ErrConnectionClosed) {
		return nil, err
	}
	mr := make([]MultiResponse, len(res.Ops))
	for i, op := range res.Ops {
		mr[i] = MultiResponse{Stat: op.Stat, String: op.String, Error: op.Err.toError()}
	}
	return mr, err
}

// MultiRead executes multiple ZooKeeper read operations at once. The provided ops must be one of GetDataOp or
// GetChildrenOp. Returns an error on network or connectivity errors, not on any op errors such as ErrNoNode. To check
// if any ops failed, check the corresponding MultiReadResponse.Err.
func (c *Conn) MultiRead(ops ...ReadOp) ([]MultiReadResponse, error) {
	req := &multiRequest{
		Ops: make([]multiRequestOp, len(ops)),
	}
	for i, op := range ops {
		req.Ops[i] = multiRequestOp{
			Header: multiHeader{op.opCode(), false, -1},
			Op:     pathWatchRequest{Path: op.GetPath()},
		}
	}
	res := &multiReadResponse{}
	_, err := c.request(opMultiRead, req, res, nil)
	return res.OpResults, err
}

// GetDataAndChildren executes a multi-read to get the given node's data and its children in one call.
func (c *Conn) GetDataAndChildren(path string) ([]byte, *Stat, []string, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	opResults, err := c.MultiRead(GetDataOp(path), GetChildrenOp(path))
	if err != nil {
		return nil, nil, nil, err
	}

	for _, r := range opResults {
		if r.Err != nil {
			return nil, nil, nil, r.Err
		}
	}

	return opResults[0].Data, opResults[0].Stat, opResults[1].Children, nil
}

// IncrementalReconfig is the zookeeper reconfiguration api that allows adding and removing servers
// by lists of members. For more info refer to the ZK documentation.
//
// An optional version allows for conditional reconfigurations, -1 ignores the condition.
//
// Returns the new configuration znode stat.
func (c *Conn) IncrementalReconfig(joining, leaving []string, version int64) (*Stat, error) {
	// TODO: validate the shape of the member string to give early feedback.
	request := &reconfigRequest{
		JoiningServers: []byte(strings.Join(joining, ",")),
		LeavingServers: []byte(strings.Join(leaving, ",")),
		CurConfigId:    version,
	}

	return c.internalReconfig(request)
}

// Reconfig is the non-incremental update functionality for Zookeeper where the list provided
// is the entire new member list. For more info refer to the ZK documentation.
//
// An optional version allows for conditional reconfigurations, -1 ignores the condition.
//
// Returns the new configuration znode stat.
func (c *Conn) Reconfig(members []string, version int64) (*Stat, error) {
	request := &reconfigRequest{
		NewMembers:  []byte(strings.Join(members, ",")),
		CurConfigId: version,
	}

	return c.internalReconfig(request)
}

func (c *Conn) internalReconfig(request *reconfigRequest) (*Stat, error) {
	response := &reconfigReponse{}
	_, err := c.request(opReconfig, request, response, nil)
	return response.Stat, err
}

// Server returns the current or last-connected server name.
func (c *Conn) Server() string {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	return c.server
}

func (c *Conn) AddPersistentWatch(path string, mode AddWatchMode) (ch EventQueue, err error) {
	if err = validatePath(path, false); err != nil {
		return nil, err
	}

	res := &addWatchResponse{}
	_, err = c.request(opAddWatch, &addWatchRequest{Path: path, Mode: mode}, res, func(r *request, header *responseHeader, err error) {
		if err == nil {
			var wt watchType
			if mode == AddWatchModePersistent {
				wt = watchTypePersistent
			} else {
				wt = watchTypePersistentRecursive
			}

			ch = NewUnlimitedQueue[Event]()
			c.addWatcher(path, wt, ch)
		}
	})
	if errors.Is(err, ErrConnectionClosed) {
		return nil, err
	}
	return ch, err
}

func (c *Conn) RemovePersistentWatch(path string, ch EventQueue) (err error) {
	if err = validatePath(path, false); err != nil {
		return err
	}

	deleted := false

	res := &checkWatchesResponse{}
	_, err = c.request(opCheckWatches, &checkWatchesRequest{Path: path, Type: WatcherTypeAny}, res, func(r *request, header *responseHeader, err error) {
		if err != nil {
			return
		}

		c.watchersLock.Lock()
		defer c.watchersLock.Unlock()

		for _, wt := range persistentWatchTypes {
			wpt := watchPathType{path: path, wType: wt}
			for i, w := range c.watchers[wpt] {
				if w == ch {
					deleted = true
					c.watchers[wpt] = append(c.watchers[wpt][:i], c.watchers[wpt][i+1:]...)
					w.Push(Event{Type: EventNotWatching, State: c.State(), Path: path, Err: ErrNoWatcher})
					w.Close()
					return
				}
			}
		}
	})

	if err != nil {
		return err
	}

	if !deleted {
		return ErrNoWatcher
	}

	return nil
}

func (c *Conn) RemoveAllPersistentWatches(path string) (err error) {
	if err = validatePath(path, false); err != nil {
		return err
	}

	res := &checkWatchesResponse{}
	_, err = c.request(opRemoveWatches, &checkWatchesRequest{Path: path, Type: WatcherTypeAny}, res, func(r *request, header *responseHeader, err error) {
		if err != nil {
			return
		}

		c.watchersLock.Lock()
		defer c.watchersLock.Unlock()
		for _, wt := range persistentWatchTypes {
			wpt := watchPathType{path: path, wType: wt}
			for _, ch := range c.watchers[wpt] {
				ch.Push(Event{Type: EventNotWatching, State: c.State(), Path: path, Err: ErrNoWatcher})
				ch.Close()
			}
			delete(c.watchers, wpt)
		}
	})
	return err
}

func resendZkAuth(ctx context.Context, c *Conn) error {
	shouldCancel := func() bool {
		select {
		case <-c.shouldQuit:
			return true
		case <-c.closeChan:
			return true
		default:
			return false
		}
	}

	c.credsMu.Lock()
	defer c.credsMu.Unlock()

	slog.Info("re-submitting credentials after reconnect", "credentialCount", len(c.creds))

	for _, cred := range c.creds {
		// return early before attempting to send request.
		if shouldCancel() {
			return nil
		}
		// do not use the public API for auth since it depends on the send/recv loops
		// that are waiting for this to return
		resChan, err := c.sendRequest(
			opSetAuth,
			&setAuthRequest{Type: 0,
				Scheme: cred.scheme,
				Auth:   cred.auth,
			},
			&setAuthResponse{},
			nil, /* recvFunc*/
		)
		if err != nil {
			return fmt.Errorf("failed to send auth request: %w", err)
		}

		var res response
		select {
		case res = <-resChan:
		case <-c.closeChan:
			slog.Info("recv closed, cancel re-submitting credentials")
			return nil
		case <-c.shouldQuit:
			slog.Warn("Connection closing, cancel re-submitting credentials")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		if res.err != nil {
			return fmt.Errorf("failed connection setAuth request: %w", res.err)
		}
	}

	return nil
}

func JoinPath(parent, child string) string {
	if !strings.HasSuffix(parent, "/") {
		parent += "/"
	}
	if strings.HasPrefix(child, "/") {
		child = child[1:]
	}
	return parent + child
}

func SplitPath(path string) (dir, name string) {
	i := strings.LastIndex(path, "/")
	if i == 0 {
		dir, name = "/", path[1:]
	} else {
		dir, name = path[:i], path[i+1:]
	}
	return dir, name
}

// IsConnClosing checks if the given error returned by a Conn function is due to Close having been
// called on the Conn, which is fatal in that no subsequent Conn operations will succeed.
func IsConnClosing(err error) bool {
	return errors.Is(err, ErrClosing)
}
