import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Mini Redis-like server (educational):
 * - Single-threaded event loop with Java NIO Selector
 * - Handles multiple clients
 * - Parses commands (inline or RESP Arrays) and executes only SET, PING, ECHO, QUIT
 * - Simple replication:
 *     - A replica connects and sends SYNC (inline or RESP). Master replies by streaming all current keys as SETs
 *       and then forwards future SETs to all replicas.
 *     - This is a toy protocol (not Redis PSYNC). For learning only.
 *
 * Run master:
 *   java MiniRedis 6379
 *
 * Run replica pointing to master:
 *   java MiniRedis 6380 --replicaof 127.0.0.1 6379
 *
 * Test (telnet/netcat):
 *   *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 * or inline:
 *   SET foo bar\r\n
 */
public class MiniRedis {
	private final int port;
	private final Map<String, String> db = new LinkedHashMap<>(); // preserves order for snapshot

	// Networking
	private Selector selector;
	private ServerSocketChannel server;

	// All client connections (regular and replicas)
	private final Map<SocketChannel, Conn> conns = new HashMap<>();

	// Replication bookkeeping
	private final Set<SocketChannel> replicas = new HashSet<>();

	// If this server is a replica, it will maintain an upstream connection to the master
	private final String upstreamHost;
	private final Integer upstreamPort;
	private SocketChannel upstream; // connection to master (if replica)
	private Conn upstreamConn;      // parser state for upstream

	public MiniRedis(int port, String upstreamHost, Integer upstreamPort) {
		this.port = port;
		this.upstreamHost = upstreamHost;
		this.upstreamPort = upstreamPort;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Usage: java MiniRedis <port> [--replicaof <host> <port>]");
			return;
		}
		int port = Integer.parseInt(args[0]);
		String uHost = null; Integer uPort = null;
		if (args.length == 4 && "--replicaof".equalsIgnoreCase(args[1])) {
			uHost = args[2];
			uPort = Integer.parseInt(args[3]);
		}
		new MiniRedis(port, uHost, uPort).run();
	}

	// Connection holder with buffering and parsing state
	private static class Conn {
		final SocketChannel ch;
		final ByteBuffer in = ByteBuffer.allocateDirect(64 * 1024);
		final Deque<ByteBuffer> outq = new ArrayDeque<>();
		boolean closing = false;
		boolean isReplica = false; // set on server-side when client says SYNC

		// RESP array parse state
		Integer respArgsExpected = null; // when parsing RESP arrays: number of bulk args expected
		final List<String> respArgs = new ArrayList<>();

		Conn(SocketChannel ch) { this.ch = ch; }

		void enqueue(String s) {
			outq.add(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
		}
	}

	public void run() throws Exception {
		selector = Selector.open();

		// Start listener
		server = ServerSocketChannel.open();
		server.configureBlocking(false);
		server.bind(new InetSocketAddress(port));
		server.register(selector, SelectionKey.OP_ACCEPT);
		log("Listening on port %d%s", port, upstreamHost == null ? "" : " (replica of " + upstreamHost + ":" + upstreamPort + ")");

		// If replica, connect to master
		if (upstreamHost != null) connectUpstream();

		// Event loop
		while (true) {
			selector.select();
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();
			while (it.hasNext()) {
				SelectionKey key = it.next();
				it.remove();
				try {
					if (!key.isValid()) continue;
					if (key.isAcceptable()) accept();
					if (key.isReadable()) read((SocketChannel) key.channel());
					if (key.isWritable()) write((SocketChannel) key.channel());
				} catch (IOException e) {
					// On IO error, close channel
					closeQuiet((SocketChannel) key.channel());
				}
			}
		}
	}

	private void connectUpstream() throws IOException {
		upstream = SocketChannel.open();
		upstream.configureBlocking(false);
		upstream.connect(new InetSocketAddress(upstreamHost, upstreamPort));
		while (!upstream.finishConnect()) { /* spin until connected */ }
		upstream.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		upstreamConn = new Conn(upstream);
		conns.put(upstream, upstreamConn);
		// Send our toy SYNC to master to start streaming
		upstreamConn.enqueue("*1\r\n$4\r\nSYNC\r\n");
		log("Connected to master %s:%d and sent SYNC", upstreamHost, upstreamPort);
	}

	private void accept() throws IOException {
		SocketChannel ch = server.accept();
		if (ch == null) return;
		ch.configureBlocking(false);
		ch.register(selector, SelectionKey.OP_READ);
		conns.put(ch, new Conn(ch));
		log("Accepted %s", ch.getRemoteAddress());
	}

	private void read(SocketChannel ch) throws IOException {
		Conn c = conns.get(ch);
		if (c == null) return;
		int n = ch.read(c.in);
		if (n == -1) { closeQuiet(ch); return; }
		if (n == 0) return;
		c.in.flip();
		// parse as many complete commands as possible
		while (true) {
			List<String> cmd = parseNextCommand(c);
			if (cmd == null) break; // need more data
			handleCommand(c, cmd);
		}
		// compact buffer for next read
		c.in.compact();
		// after enqueueing responses, ensure channel is interested in WRITE
		SelectionKey key = ch.keyFor(selector);
		if (key != null && key.isValid()) key.interestOps(key.interestOps() | SelectionKey.OP_WRITE | SelectionKey.OP_READ);
	}

	private void write(SocketChannel ch) throws IOException {
		Conn c = conns.get(ch);
		if (c == null) return;
		while (!c.outq.isEmpty()) {
			ByteBuffer buf = c.outq.peek();
			ch.write(buf);
			if (buf.hasRemaining()) break; // kernel buffer full
			c.outq.poll();
		}
		SelectionKey key = ch.keyFor(selector);
		if (key != null && key.isValid() && c.outq.isEmpty()) {
			// remove WRITE interest if nothing to write
			key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
			if (c.closing) closeQuiet(ch);
		}
	}

	private void handleCommand(Conn c, List<String> args) throws IOException {
		if (args.isEmpty()) return;
		String cmd = args.get(0).toUpperCase(Locale.ROOT);
		switch (cmd) {
			case "PING":
				c.enqueue("+PONG\r\n");
				break;
			case "ECHO":
				String msg = args.size() >= 2 ? args.get(1) : "";
				c.enqueue(bulk(msg));
				break;
			case "QUIT":
				c.enqueue("+OK\r\n");
				c.closing = true;
				break;
			case "SET":
				if (args.size() < 3) { c.enqueue("-ERR wrong number of arguments for 'SET'\r\n"); break; }
				String key = args.get(1);
				String value = args.get(2);
				db.put(key, value);
				c.enqueue("+OK\r\n");
				// Propagate to replicas
				broadcastToReplicas(respArray("SET", key, value));
				break;
			case "SYNC":
				// Mark this connection as a replica and send a naive full snapshot
				c.isReplica = true;
				if (!replicas.contains(c.ch)) replicas.add(c.ch);
				// stream current state as SETs
				for (Map.Entry<String,String> e : db.entrySet()) {
					c.enqueue(respArray("SET", e.getKey(), e.getValue()));
				}
				// No explicit OK; replication is just a stream
				break;
			default:
				c.enqueue("-ERR unknown command '" + args.get(0) + "'\r\n");
		}
	}

	private void broadcastToReplicas(String payload) {
		if (replicas.isEmpty()) return;
		ByteBuffer buf = ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8));
		for (SocketChannel r : new ArrayList<>(replicas)) {
			Conn rc = conns.get(r);
			if (rc == null) continue;
			rc.outq.add(buf.duplicate()); // each replica gets its own view
			SelectionKey key = r.keyFor(selector);
			if (key != null && key.isValid()) key.interestOps(key.interestOps() | SelectionKey.OP_WRITE | SelectionKey.OP_READ);
		}
		selector.wakeup();
	}

	// --- RESP / inline parsing ---
	private List<String> parseNextCommand(Conn c) {
		c.in.mark();
		if (!c.in.hasRemaining()) return null;
		byte b = c.in.get(c.in.position());
		if (b == '*') {
			// RESP Array: *<n>\r\n$<len>\r\n<arg>\r\n ...
			return parseRespArray(c);
		} else {
			// Inline: up to CRLF
			return parseInline(c);
		}
	}

	private List<String> parseInline(Conn c) {
		int start = c.in.position();
		int end = -1;
		while (c.in.hasRemaining()) {
			byte b = c.in.get();
			if (b == '\r') {
				if (c.in.hasRemaining()) {
					byte n = c.in.get();
					if (n == '\n') { end = c.in.position(); break; }
				} else { break; }
			}
		}
		if (end == -1) { c.in.position(start); return null; } // incomplete line
		int len = (end - start) - 2; // strip CRLF
		byte[] bytes = new byte[len];
		c.in.position(start);
		c.in.get(bytes, 0, len);
		c.in.get(); c.in.get(); // consume CRLF
		String line = new String(bytes, StandardCharsets.UTF_8).trim();
		if (line.isEmpty()) return Collections.emptyList();
		return Arrays.asList(line.split("\\s+"));
	}

	private List<String> parseRespArray(Conn c) {
		// Save starting mark
		int markPos = c.in.position();
		// Read '*' and count
		if (c.in.remaining() < 3) return resetAndNull(c, markPos);
		if (c.in.get() != '*') return resetAndNull(c, markPos);
		Integer count = readIntegerUntilCRLF(c);
		if (count == null) return resetAndNull(c, markPos);
		List<String> out = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			if (!c.in.hasRemaining()) return resetAndNull(c, markPos);
			byte sig = c.in.get();
			if (sig != '$') return resetAndNull(c, markPos);
			Integer blen = readIntegerUntilCRLF(c);
			if (blen == null) return resetAndNull(c, markPos);
			if (c.in.remaining() < blen + 2) return resetAndNull(c, markPos);
			byte[] data = new byte[blen];
			c.in.get(data);
			// expect CRLF
			byte cr = c.in.get(); byte lf = c.in.get();
			if (cr != '\r' || lf != '\n') return resetAndNull(c, markPos);
			out.add(new String(data, StandardCharsets.UTF_8));
		}
		return out;
	}

	private Integer readIntegerUntilCRLF(Conn c) {
		StringBuilder sb = new StringBuilder();
		while (c.in.hasRemaining()) {
			byte b = c.in.get();
			if (b == '\r') {
				if (!c.in.hasRemaining()) return null; // incomplete
				byte n = c.in.get();
				if (n != '\n') return null;
				try { return Integer.parseInt(sb.toString()); } catch (NumberFormatException e) { return null; }
			} else {
				sb.append((char) b);
			}
		}
		return null; // incomplete
	}

	private List<String> resetAndNull(Conn c, int pos) {
		c.in.position(pos);
		return null;
	}

	// RESP helpers
	private static String bulk(String s) {
		byte[] b = s.getBytes(StandardCharsets.UTF_8);
		return "$" + b.length + "\r\n" + s + "\r\n";
	}
	private static String respArray(String... parts) {
		StringBuilder sb = new StringBuilder();
		sb.append("*").append(parts.length).append("\r\n");
		for (String p : parts) sb.append(bulk(p));
		return sb.toString();
	}

	private void closeQuiet(SocketChannel ch) {
		try {
			if (ch == null) return;
			replicas.remove(ch);
			conns.remove(ch);
			ch.close();
			//log("Closed %s", ch);
		} catch (Exception ignored) {}
	}

	private static void log(String fmt, Object... a) {
		System.out.println("[MiniRedis] " + String.format(fmt, a));
	}
}
