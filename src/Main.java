import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {
	private static final HashMap<String, String[]> map = new HashMap<>();
	private static final HashMap<String, Stream> streams = new HashMap<>();
	private static final HashMap<String, String> redisKeys = new HashMap<>();
	private static final HashMap<String, LinkedList<ArrayList<String>>> lists = new HashMap<>();
	private static final ZSET zset = new ZSET();
	private static String masterHost;
	private static int masterPort;
	private static int replicaPort;
	private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	private static long master_repl_offset = 0;
	private static long replica_repl_offset = 0;
	private static boolean isReplica = false;
	private static final Set<String> writeCmds = new HashSet<>(Arrays.asList("SET", "INCR"));
	private static final Set<SelectionKey> replicaWriters = new HashSet<>();
	private static final ConcurrentHashMap<SocketChannel, Long> replicaOffsets = new ConcurrentHashMap<>();
	private static String dir;
	private static String dbfilename;
	private static final HashMap<String, Set<SelectionKey>> channelToSubs = new HashMap<>();
	private static final HashMap<SelectionKey, Set<String>> subToChannels = new HashMap<>();
	private static final Set<String> subModeCmds = new HashSet<>(Arrays.asList("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"));
	private static final Set<SocketChannel> clients = new HashSet<>();
	private static final HashMap<SelectionKey, MultiHandler> multiHandlers = new HashMap<>();
	private static final ConcurrentHashMap<String, Queue<ClientRequest>> waitingQueue = new ConcurrentHashMap<>();
	private static ConcurrentLinkedQueue<StreamRequest> streamWaitQueue = new ConcurrentLinkedQueue<>();
	private static final ConcurrentLinkedQueue<AckRequest> ackWaitQueue = new ConcurrentLinkedQueue<>();
	private static final HashMap<String, List<Geo>> geo = new HashMap<>();

	public static void main(String[] args) {
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		System.out.println("Logs from your program will appear here!");

		int port = 6379;
		try {
			// Parse command-line arguments
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("--port") && i + 1 < args.length) {
					port = Integer.parseInt(args[++i]);
				} else if (args[i].equals("--replicaof") && i + 1 < args.length) {
					String[] masterInfo = args[++i].split(" ");
					masterHost = masterInfo[0];
					masterPort = Integer.parseInt(masterInfo[1]);
					isReplica = true;
					replicaPort = port;
				} else if (args[i].equals("--dir") && i + 1 < args.length) {
					dir = args[++i];
				} else if (args[i].equals("--dbfilename") && i + 1 < args.length) {
					dbfilename = args[++i];
				}
			}

			if (dir != null && dbfilename != null) {
				loadRDB();
			}

			// Code block - event loop set up
			Selector selector = Selector.open();
			ServerSocketChannel ssc = ServerSocketChannel.open();
			ssc.configureBlocking(false);
			ssc.bind(new InetSocketAddress(port));
			ssc.register(selector, SelectionKey.OP_ACCEPT);

			if (isReplica) {
				new Thread(new MasterConnectionHandler(masterHost, masterPort)).start();
			}

			new Thread(new WaitingQueueHandler()).start();
			new Thread(new StreamWaitingQueueHandler()).start();
			new Thread(new AckWaitingQueueHandler()).start();

			// Start listening to connections
			while (true) {
				try {
					selector.select();
					Set<SelectionKey> keys = selector.selectedKeys();
					Iterator<SelectionKey> it = keys.iterator();
					while (it.hasNext()) {
						SelectionKey key = it.next();
						if (key.isAcceptable()) {
							acceptConnection(selector, key);
						} else if (key.isReadable()) {
							readCommand(key);
						}
						it.remove();
					}
				} catch (IOException e) {
					throw new RuntimeException("-ERR connection error");
				}
			}

		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		} finally {
			try {
				for (SocketChannel channel : clients) {
					channel.close();
				}
			} catch (IOException e) {
				System.out.println("IOException: " + e.getMessage());
			}
		}
	}

	static class MasterConnectionHandler implements Runnable {

		private final String host;
		private final int port;

		MasterConnectionHandler(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public void run() {
			try (Socket masterSocket = new Socket(host, port)) {
				InputStream in = masterSocket.getInputStream();
				OutputStream out = masterSocket.getOutputStream();
				String resp = "*1\r\n$4\r\nPING\r\n";
				out.write(resp.getBytes());
				out.flush();
				readLine(in);

				String listeningPort =
						"*3\r\n" +
								"$8\r\nREPLCONF\r\n" +
								"$14\r\nlistening-port\r\n" +
								"$" + String.valueOf(replicaPort).length() + "\r\n" + replicaPort + "\r\n";
				out.write(listeningPort.getBytes());
				out.flush();
				readLine(in);

				String capa =
						"*3\r\n" +
								"$8\r\nREPLCONF\r\n" +
								"$4\r\ncapa\r\n" +
								"$6\r\npsync2\r\n";
				out.write(capa.getBytes());
				out.flush();
				readLine(in);

				String psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
				out.write(psync.getBytes());
				out.flush();
				String fullresync = readLine(in);

				String lenLine = readLine(in);
				int len = Integer.parseInt(lenLine.substring(1));
				byte[] rdb = new byte[len];
				int read = 0;
				while (read < len) {
					int n = in.read(rdb, read, len - read);
					if (n <= 0) throw new IOException("Failed to read RDB");
					read += n;
				}

				while (true) {
					List<String> command = parseRESP(in);
					if (command == null || command.isEmpty()) continue;
					String cmd = command.get(0).toUpperCase();
					if (cmd.equals("REPLCONF") && command.size() > 1 && command.get(1).equalsIgnoreCase("GETACK")) {
						String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n";
						String offset = String.valueOf(replica_repl_offset);
						ack += "$" + offset.length() + "\r\n" + offset + "\r\n";
						out.write(ack.getBytes());
						out.flush();
					} else {
						execCommand(command, null, null);
					}
					replica_repl_offset += getResp(command).getBytes().length;
				}
			} catch (IOException e) {
				System.out.println("IOException: " + e.getMessage());
			}
		}
	}

	private static String readLine(InputStream in) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while (true) {
			int b = in.read();
			if (b == -1) {
				if (baos.size() == 0) return null;
				throw new EOFException("Unexpected EOF while reading line");
			}
			if (b == '\r') {
				int next = in.read();
				if (next == -1) {
					throw new EOFException();
				}
				if (next == '\n') {
					break;
				}
				baos.write(b);
				baos.write(next);
				continue;
			}
			baos.write(b);
		}
		return new String(baos.toByteArray(), "UTF-8");
	}

	private static List<String> parseRESP(InputStream in) throws IOException {
		String firstLine = readLine(in);
		if (firstLine == null) return null;

		if (firstLine.startsWith("*")) {
			int argCount = Integer.parseInt(firstLine.substring(1));
			List<String> args = new ArrayList<>();
			for (int i = 0; i < argCount; i++) {
				String lenLine = readLine(in);
				if (!lenLine.startsWith("$")) {
					throw new IOException("Invalid bulk string length line: " + lenLine);
				}
				int length = Integer.parseInt(lenLine.substring(1));
				if (length == -1) {
					args.add(null);
					readLine(in); // \r\n
					continue;
				}
				byte[] buf = new byte[length];
				int r = 0;
				while (r < length) {
					int n = in.read(buf, r, length - r);
					if (n <= 0) throw new IOException("Failed to read bulk string");
					r += n;
				}
				args.add(new String(buf, "UTF-8"));
				String crlf = readLine(in);
				if (!crlf.isEmpty()) {
					throw new IOException("Expected \r\n after bulk string");
				}
			}
			return args;
		} else {
			throw new IOException("Invalid RESP input: " + firstLine);
		}
	}


	private static void acceptConnection(Selector selector, SelectionKey key) {
		try {
			ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
			SocketChannel client = ssc.accept();
			client.configureBlocking(false);
			client.register(selector, SelectionKey.OP_READ);
			clients.add(client);
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}
	}

	private static void writeResponseToClient (String resp, SelectionKey key) {
		SocketChannel client = (SocketChannel) key.channel();
		try {
			ByteBuffer response = ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8));
			while (response.hasRemaining()) {
				client.write(response);
			}
		} catch (IOException e) {
			try {
				clients.remove(client);
				client.close();
				key.cancel();
			} catch (IOException ignored) {}
			System.out.println("IOException: " + e.getMessage());
		}

	}

	private static void readCommand(SelectionKey key) {
		SocketChannel client = (SocketChannel) key.channel();
		try {
			StringBuilder sb = (StringBuilder) key.attachment();
			if (sb == null) {
				sb = new StringBuilder();
				key.attach(sb);
			}
			ByteBuffer buffer = ByteBuffer.allocate(1024);

			int bytesRead = client.read(buffer);
			if (bytesRead == -1) {
				clients.remove(client);
				key.cancel();
				client.close();
				return;
			}

			if (bytesRead > 0) {
				buffer.flip();
				byte[] bytes = new byte[buffer.remaining()];
				buffer.get(bytes);
				String msg = new String(bytes, StandardCharsets.UTF_8);
				sb.append(msg);
				buffer.clear();

				while (true) {
					List<String> command = parseRESP(sb);
					System.out.println("command being read: " + command);
					if (command != null && !command.isEmpty()) {
						String cmd = command.get(0).toUpperCase();

						if (isSubMode(key) && !subModeCmds.contains(cmd)) {
							String resp = "-ERR Can't execute '" + cmd + "' in subscribed mode\r\n";
							writeResponseToClient(resp, key);
							continue;
						}


						if  (cmd.equals("UNSUBSCRIBE")) {
							String channel = command.get(1);

							if (channelToSubs.containsKey(channel)) {
								channelToSubs.get(channel).remove(key);
								if  (channelToSubs.get(channel).isEmpty()) {
									channelToSubs.remove(channel);
								}
							}


							if (subToChannels.containsKey(key)) {
								subToChannels.get(key).remove(channel);
								if  (subToChannels.get(key).isEmpty()) {
									subToChannels.remove(key);
								}
							}
							int channelCount = subToChannels.containsKey(key) ? subToChannels.get(key).size() : 0;
							String resp = "*3"+ "\r\n" + "$11" + "\r\n" + "unsubscribe" + "\r\n";
							resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
							resp += ":" + channelCount + "\r\n";
							writeResponseToClient(resp, key);
						} else if  (cmd.equals("SUBSCRIBE")) {
							String channel = command.get(1);
							channelToSubs.putIfAbsent(channel, new HashSet<>());
							channelToSubs.get(channel).add(key);

							subToChannels.putIfAbsent(key, new HashSet<>());
							subToChannels.get(key).add(channel);

							int channelCount = subToChannels.get(key).size();
							String resp = "*3"+ "\r\n" + "$9" + "\r\n" + "subscribe" + "\r\n";
							resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
							resp += ":" + channelCount + "\r\n";
							writeResponseToClient(resp, key);
						} else if (cmd.equals("CONFIG")) {
							if (command.size() == 3 && command.get(1).equalsIgnoreCase("get") && command.get(2).equalsIgnoreCase("dir")) {
								String dirValue = dir != null ? dir : "";
								String resp = "*2\r\n$3\r\ndir\r\n$" + dirValue.length() + "\r\n" + dirValue + "\r\n";
								writeResponseToClient(resp, key);
							}
						} else if (cmd.equals("PSYNC")) {
							String resp = "+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n";
							writeResponseToClient(resp, key);

							byte[] rdb = new RDB().getRDB();
							String lengthHeader = "$" + rdb.length + "\r\n";
							writeResponseToClient(lengthHeader, key);
							ByteBuffer bb = ByteBuffer.wrap(rdb);
							while (bb.hasRemaining()) {
								client.write(bb);
							}
						} else if (cmd.equals("REPLCONF")) {
							boolean sendOk = true;
							if (command.size() > 2 && command.get(1).toUpperCase().equals("ACK")) {
								long offset = Long.parseLong(command.get(2));
								System.out.println("setting offset after replica send ack message: " + client);
								System.out.println("offset: " + offset);
								replicaOffsets.put(client, offset);
								sendOk = false;
							}
							if (sendOk) {
								String resp = "+OK\r\n";
								writeResponseToClient(resp, key);
							}
							if (command.size() > 2 && command.get(1).equalsIgnoreCase("listening-port")) {
								replicaWriters.add(key);
							}
						} else if (cmd.equals("INFO")) {
							if (command.get(1).equalsIgnoreCase("replication")) {
								String role = isReplica ? "slave" : "master";
								String infoRepl = "+role:" + role + "\r\n" + "+master_replid:" + master_replid + "\r\n" + "+master_repl_offset:" + master_repl_offset + "\r\n";
								String resp = "$" + infoRepl.length() + "\r\n" + infoRepl + "\r\n";
								writeResponseToClient(resp, key);
							}
						} else if (cmd.equals("MULTI")) {
							System.out.println("Selection key: " + key);
							multiHandlers.putIfAbsent(key, new MultiHandler());
							multiHandlers.get(key).init();
							String resp = "+OK\r\n";
							writeResponseToClient(resp, key);
						} else if (cmd.equals("DISCARD")) {
							if (!multiHandlers.containsKey(key) || !multiHandlers.get(key).isOn()) {
								writeResponseToClient("-ERR DISCARD without MULTI\r\n", key);
							} else {
								multiHandlers.get(key).clear();
								writeResponseToClient("+OK\r\n", key);
							}
						} else if (cmd.equals("EXEC")) {
							if (!multiHandlers.containsKey(key) || !multiHandlers.get(key).isOn()) {
								writeResponseToClient("-ERR EXEC without MULTI\r\n", key);
							} else if (multiHandlers.get(key).isEmpty()) {
								multiHandlers.get(key).clear();
								writeResponseToClient("*0\r\n", key);
							} else {
								String resp = "*" + multiHandlers.get(key).getSize() + "\r\n";
								while (!multiHandlers.get(key).isEmpty()) {
									List<String> args = multiHandlers.get(key).getNext();
									String res = execCommand(args, client, key);
									if (res == null) {
										throw new IOException("unknown command: " + command);
									} else {
										resp += res;
									}
								}
								writeResponseToClient(resp, key);
								multiHandlers.get(key).clear();
							}
						} else {
							if (multiHandlers.containsKey(key) && multiHandlers.get(key).isOn()) {
								multiHandlers.get(key).add(command);
								writeResponseToClient("+QUEUED\r\n", key);
							} else {
								String resp = execCommand(command, client, key);
								System.out.println("result: " + resp);
								if (resp == null) throw new IOException("unknown command: " + command);
								if (resp.equals("wait")) break;
								writeResponseToClient(resp, key);
							}
						}
						if (writeCmds.contains(cmd)) {
							String resp = getResp(command);
							master_repl_offset += resp.getBytes().length;
							for (SelectionKey skey : replicaWriters) {
								writeResponseToClient(resp, skey);
							}
						}
					} else break;
				}
			}

		} catch (IOException e) {
			try	{
				replicaWriters.remove(key);
				replicaOffsets.remove(client);

				clients.remove(client);
				key.cancel();
				client.close();
			} catch (IOException ignored) {}
			System.out.println("IOException: " + e.getMessage());
		}
	}

	// Load RDB file and populate map with key-value pair
	private static void loadRDB() {
		File file = new File(dir, dbfilename);
		if (!file.exists()) {
			System.out.println("RDB file does not exist: " + file.getPath());
			return;
		}
		try (FileInputStream fis = new FileInputStream(file)) {
			byte[] header = new byte[9];
			if (fis.read(header) != 9 || !new String(header).equals("REDIS0011")) {
				throw new IOException("Invalid RDB header");
			}
			System.out.println("Header read: " + new String(header));
			while (true) {
				int opcode = fis.read();
				if (opcode == -1 || opcode == 0xFF) {
					System.out.println("Reached EOF or FF, breaking");
					break;
				}
				System.out.println("Opcode: 0x" + Integer.toHexString(opcode));
				if (opcode == 0xFA) {
					System.out.println("Processing metadata");
					long nameLength = readLength(fis);
					System.out.println("Metadata name length: " + nameLength);
					if (nameLength < 0 || nameLength > Integer.MAX_VALUE) {
						System.out.println("Invalid or EOF name length: " + nameLength);
						break;
					}
					fis.skip(nameLength);
					long valueLength = readLength(fis);
					System.out.println("Metadata value length: " + valueLength);
					if (valueLength >= 0 && valueLength <= Integer.MAX_VALUE) {
						fis.skip(valueLength);
					} else if (valueLength == -2) {
						System.out.println("Special encoding detected, no value skip");
					} else {
						System.out.println("Invalid or EOF value length: " + valueLength);
						break;
					}
				} else if (opcode == 0xFE) {
					System.out.println("Processing database section");
					long dbIndex = readLength(fis);
					System.out.println("Database index: " + dbIndex);
					int fb = fis.read();
					if (fb != 0xFB) throw new IOException("Expected FB, got 0x" + Integer.toHexString(fb));
					long hashTableSize = readLength(fis);
					long expireTableSize = readLength(fis);
					System.out.println("Hash table size: " + hashTableSize + ", Expire table size: " + expireTableSize);
					for (long i = 0; i < hashTableSize; i++) {
						int typeOrExpiry = fis.read();
						if (typeOrExpiry == -1) {
							System.out.println("EOF at typeOrExpiry, breaking");
							break;
						}
						System.out.println("Type or expiry: 0x" + Integer.toHexString(typeOrExpiry));
						long expiry = -1;
						if (typeOrExpiry == 0xFD) {
							expiry = readUnsignedInt(fis) * 1000L;
							typeOrExpiry = fis.read();
							if (typeOrExpiry == -1) {
								System.out.println("EOF after FD expiry, breaking");
								break;
							}
							System.out.println("FD expiry: " + expiry + ", next type: 0x" + Integer.toHexString(typeOrExpiry));
						} else if (typeOrExpiry == 0xFC) {
							expiry = readUnsignedLong(fis);
							typeOrExpiry = fis.read();
							if (typeOrExpiry == -1) {
								System.out.println("EOF after FC expiry, breaking");
								break;
							}
							System.out.println("FC expiry: " + expiry + ", next type: 0x" + Integer.toHexString(typeOrExpiry));
						}
						if (typeOrExpiry != 0x00) {
							throw new IOException("Unsupported value type: 0x" + Integer.toHexString(typeOrExpiry));
						}
						String key = readString(fis);
						String value = readString(fis);
						System.out.println("Read key: '" + key + "', value: '" + value + "', expiry: " + expiry);
						if (!key.isEmpty()) {
							map.put(key, new String[]{value, String.valueOf(expiry)});
							System.out.println("Stored key: " + key + " in map");
						} else {
							System.out.println("Skipping empty key");
						}
					}
				}
			}
			System.out.println("Map after loadRDB: " + map);
		} catch (IOException e) {
			System.out.println("Error reading RDB file: " + e.getMessage());
		}
	}

	private static String readString(FileInputStream fis) throws IOException {
		long length = readLength(fis);
		if (length == -1) {
			System.out.println("readString: EOF, returning empty string");
			return "";
		}
		if (length >= 0 && length <= Integer.MAX_VALUE) {
			byte[] bytes = new byte[(int) length];
			if (fis.read(bytes) != length) {
				System.out.println("readString: Incomplete read for length " + length);
				throw new IOException("Incomplete string read");
			}
			String result = new String(bytes, "UTF-8");
			System.out.println("readString: Read string '" + result + "' of length " + length);
			return result;
		}
		throw new IOException("Invalid string length: " + length);
	}

	// Read length-encoded value (big-endian)
	private static long readLength(FileInputStream fis) throws IOException {
		int firstByte = fis.read();
		if (firstByte == -1) {
			System.out.println("readLength: EOF");
			return -1;
		}
		int prefix = (firstByte >> 6) & 0x03;
		System.out.println("readLength: firstByte=0x" + Integer.toHexString(firstByte) + ", prefix=" + prefix);
		switch (prefix) {
			case 0:
				long len0 = firstByte & 0x3F;
				System.out.println("readLength: 6-bit length=" + len0);
				return len0;
			case 1:
				int nextByte = fis.read();
				if (nextByte == -1) {
					System.out.println("readLength: EOF in 14-bit length");
					return -1;
				}
				long len1 = ((firstByte & 0x3F) << 8) | nextByte;
				System.out.println("readLength: 14-bit length=" + len1);
				return len1;
			case 2:
				long len2 = readUnsignedInt(fis);
				System.out.println("readLength: 32-bit length=" + len2);
				return len2;
			case 3:
				int format = firstByte & 0x3F;
				if (format == 0x00) {
					int value = fis.read();
					if (value == -1) {
						System.out.println("readLength: EOF in 8-bit integer");
						return -1;
					}
					System.out.println("readLength: 8-bit integer=" + value);
					return -2; // Indicate special encoding
				}
				throw new IOException("Unsupported special string encoding: " + format);
			default:
				throw new IOException("Invalid length prefix: " + prefix);
		}
	}

	// Read 4-byte unsigned integer (big-endian)
	private static long readUnsignedInt(FileInputStream fis) throws IOException {
		byte[] bytes = new byte[4];
		if (fis.read(bytes) != 4) return -1;
		return ((bytes[0] & 0xFFL) << 24) | ((bytes[1] & 0xFFL) << 16) |
				((bytes[2] & 0xFFL) << 8) | (bytes[3] & 0xFFL);
	}

	// Read 8-byte unsigned long (little-endian for expiry)
	private static long readUnsignedLong(FileInputStream fis) throws IOException {
		byte[] bytes = new byte[8];
		if (fis.read(bytes) != 8) return -1;
		return ((bytes[7] & 0xFFL) << 56) | ((bytes[6] & 0xFFL) << 48) |
				((bytes[5] & 0xFFL) << 40) | ((bytes[4] & 0xFFL) << 32) |
				((bytes[3] & 0xFFL) << 24) | ((bytes[2] & 0xFFL) << 16) |
				((bytes[1] & 0xFFL) << 8) | (bytes[0] & 0xFFL);
	}

	static class AckWaitingQueueHandler implements Runnable {
		public void run() {
			while (true) {
				synchronized (ackWaitQueue) {
					ConcurrentLinkedQueue<AckRequest> nextq = new ConcurrentLinkedQueue<>();
					while (!ackWaitQueue.isEmpty()) {
						AckRequest ackr = ackWaitQueue.poll();
						SelectionKey skey = ackr.getSkey();
						long exp = ackr.getExp();
						int requiredAckCount = ackr.getRequiredCount();

						// Enumerate ack count
						int ackCount = 0;
						for (long offset : replicaOffsets.values()) {
							if (offset >= master_repl_offset) ackCount++;
						}

						if (ackCount >= requiredAckCount || System.currentTimeMillis() >= exp) {
							String resp = ":" + ackCount + "\r\n";
							writeResponseToClient(resp, skey);
						} else {
							nextq.offer(ackr);
						}
					}
					ackWaitQueue.addAll(nextq);
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	static class StreamWaitingQueueHandler implements Runnable {

		public void run() {
			while (true) {
				synchronized (streamWaitQueue) {
					ConcurrentLinkedQueue<StreamRequest> nextQ = new ConcurrentLinkedQueue<>();
					while (!streamWaitQueue.isEmpty()) {
						StreamRequest sr = streamWaitQueue.poll();
						long exp = sr.getExp();
						SelectionKey skey = sr.getSelectKey();
						if (exp != 0 && System.currentTimeMillis() >= exp) {
							writeResponseToClient("*-1\r\n", skey);
							continue;
						}
						List<String> keys = sr.getStreamKeys();
						int size = sr.getSize();
						int[] sentinels = sr.getSentinels();
						String resp = "";
						int count = 0;
						for (int i = 0; i < size; i++) {
							String key = keys.get(i);
							String hashResp = "*2\r\n";
							hashResp += "$" + key.length() + "\r\n" + key + "\r\n";
							hashResp += "*1\r\n" + "*2\r\n";
							String entryHash = streams.get(key).getNewEntries(sentinels[i]+1);
							if (!entryHash.isEmpty()) {
								resp += (hashResp + entryHash);
								count++;
							}
						}

						if (count > 0) {
							resp = "*" + count + "\r\n" + resp;
							writeResponseToClient(resp, skey);
						} else {
							nextQ.offer(sr);
						}

					}
					streamWaitQueue.addAll(nextQ);

				}

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	static class WaitingQueueHandler implements Runnable {

		public void run() {
			while (true) {
				synchronized (waitingQueue) {
					for (String key : waitingQueue.keySet()) {
						Queue<ClientRequest> q = waitingQueue.get(key);
						if (q.isEmpty()) {
							waitingQueue.remove(key);
							continue;
						}
						while (!q.isEmpty()) {
							ClientRequest req = q.peek();
							SelectionKey skey = req.getKey();
							long exp = req.getExpiryTime();
							if (exp != 0 && System.currentTimeMillis() >= exp) {
								q.poll();
								writeResponseToClient("*-1\r\n", skey);
								skey.interestOps(SelectionKey.OP_READ);
							}
							break;
						}
					}
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	// Parse RESP from the buffer, return null if incomplete
	private static List<String> parseRESP(StringBuilder sb) {
		if (sb.isEmpty())
			return null;

		if (sb.charAt(0) == '*') {
			int lineEnd = sb.indexOf("\r\n");
			if (lineEnd == -1)
				return null;

			int numElements;
			try {
				numElements = Integer.parseInt(sb.substring(1, lineEnd));
			} catch (NumberFormatException e) {
				return null; // malformed header
			}

			List<String> parts = new ArrayList<>();
			int pos = lineEnd + 2;

			for (int i = 0; i < numElements; i++) {
				if (pos >= sb.length() || sb.charAt(pos) != '$')
					return null;

				int lenEnd = sb.indexOf("\r\n", pos);
				if (lenEnd == -1)
					return null;

				int bulkLen;
				try {
					bulkLen = Integer.parseInt(sb.substring(pos + 1, lenEnd));
				} catch (NumberFormatException e) {
					return null;
				}
				pos = lenEnd + 2;

				if (bulkLen < 0) {
					// NULL bulk string ($-1), represent as null
					parts.add(null);
					continue;
				}

				if (pos + bulkLen + 2 > sb.length())
					return null; // not all bytes arrived yet

				String bulkStr = sb.substring(pos, pos + bulkLen);
				parts.add(bulkStr);

				pos += bulkLen + 2; // skip bulk data and trailing \r\n
			}

			sb.delete(0, pos); // remove parsed command
			return parts;
		} else {
			// Inline command (like "PING\r\n")
			int lineEnd = sb.indexOf("\r\n");
			if (lineEnd == -1)
				return null;
			String line = sb.substring(0, lineEnd).trim();
			sb.delete(0, lineEnd + 2);
			if (line.isEmpty()) return Collections.emptyList();
			return Arrays.asList(line.split("\\s+"));
		}
	}

	private static boolean isExpired(String val) {
		if (Long.parseLong(val) == -1) return false;
		long currentTime = System.currentTimeMillis();
		if (currentTime >= Long.parseLong(val)) return true;
		return false;
	}

	private static boolean isInteger(String s) {
		if (s == null) return false;
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private static boolean set(List<String> command) {
		try {
			String key = command.get(1);
			String value = "0";
			if (command.size() > 2) {
				value = command.get(2);
			}
			long expTime;
			if (command.size() > 3 && command.get(3).equalsIgnoreCase("px")) {
				expTime = System.currentTimeMillis() + (long) Integer.parseInt(command.get(4));
			} else {
				expTime = -1;
			}
			String[] values = {value, String.valueOf(expTime)};
			map.put(key, values);
			redisKeys.put(key, "string");
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static boolean isSubMode(SelectionKey skey) {
		return subToChannels.containsKey(skey);
	}

	private static int getListSize(LinkedList<ArrayList<String>> linkList) {
		int linkSize = linkList.size();
		int entryCount;
		if (linkSize == 1) entryCount = linkList.getLast().size();
		else if (linkSize == 2) entryCount = linkList.getLast().size() + linkList.getFirst().size();
		else entryCount = linkList.getFirst().size() + linkList.getLast().size() + (100*(linkSize-2));
		return entryCount;
	}

	private static String execCommand(List<String> command, SocketChannel s, SelectionKey selectKey) {

		try {
			String cmd = command.get(0).toUpperCase();
			if (cmd.equals("PUBLISH")) {
				String channel = command.get(1);
				String msg = command.get(2);
				int subCount = 0;

				Set<SelectionKey> channels = channelToSubs.get(channel);
				String resp = "*3\r\n$7\r\nmessage\r\n";
				resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
				resp += "$" + msg.length() + "\r\n" + msg + "\r\n";
				for  (SelectionKey skey : channels) {
					writeResponseToClient(resp, skey);
				}
				subCount = channelToSubs.get(channel).size();

				return ":" + subCount + "\r\n";
			} else if (cmd.equals("PING")) {
				if (s != null && isSubMode(selectKey)) {
					String payload = command.size() > 1 ? command.get(1) : null;
					String resp = "*2" + "\r\n" + "$4\r\n" + "pong" + "\r\n";
					resp += payload == null ? "$0\r\n\r\n" : "$" + payload.length() + "\r\n" + payload + "\r\n";
					return resp;
				}
				return "+PONG\r\n";
			} else if (cmd.equals("ECHO")) {
				String arg = command.get(1);
				return "$" + arg.length() + "\r\n" + arg + "\r\n";
			} else if (cmd.equals("SET")) {
				if (set(command)) {
					return "+OK\r\n";
				} else {
					return null;
				}
			} else if (cmd.equals("ZADD")) {
				String group = command.get(1);
				double score = Double.parseDouble(command.get(2));
				String member = command.get(3);
				KeySet keySet = new KeySet(member, score);
				synchronized (zset) {
					zset.getMemberToScoreMap().putIfAbsent(group, new HashMap<>());
					zset.getOrderedListOfScores().putIfAbsent(group, new ArrayList<>());
					if (zset.getMemberToScoreMap().get(group).containsKey(member)) {
						double oldScore = zset.getMemberToScoreMap().get(group).get(member);
						zset.getOrderedListOfScores().get(group).remove(new KeySet(member, oldScore));
						zset.getOrderedListOfScores().get(group).add(keySet);
						zset.getOrderedListOfScores().get(group).sort(Comparator.comparingDouble(KeySet::getScore).thenComparing(KeySet::getMember));
						zset.getMemberToScoreMap().get(group).put(member, score);
						return ":0\r\n";
					} else {
						zset.getMemberToScoreMap().get(group).put(member, score);
						zset.getOrderedListOfScores().get(group).add(keySet);
						zset.getOrderedListOfScores().get(group).sort(Comparator.comparing(KeySet::getScore).thenComparing(KeySet::getMember));
						redisKeys.put(group, "zset");
						return ":1\r\n";
					}
				}
			} else if (cmd.equals("ZRANK")) {
				String group = command.get(1);
				String member = command.get(2);
				synchronized (zset) {
					try {
						ArrayList<KeySet> scoresOrderList = zset.getGroupOrderList(group);
						double score = zset.getScoreMap(group).get(member);
						KeySet keySet = new KeySet(member, score);
						int rank = zset.getIndex(keySet, group);
						int n = scoresOrderList.size();
						while (rank < n) {
							if (scoresOrderList.get(rank).getMember().equals(member)) break;
							rank++;
							if (scoresOrderList.get(rank).getScore() > score) {
								rank = -1;
								break;
							}
						}
						return ":" + rank + "\r\n";
					} catch (Exception e) {
						return "$-1\r\n";
					}
				}
			} else if (cmd.equals("ZRANGE")) {
				String group = command.get(1);
				int start = Integer.parseInt(command.get(2));
				int end = Integer.parseInt(command.get(3));
				synchronized (zset) {
					try {
						ArrayList<KeySet> scoresOrderList = zset.getGroupOrderList(group);
						int n = scoresOrderList.size();
						if (start < 0) start = n+start;
						if (start < 0) start = 0;
						if (end < 0) end = n+end;
						if (end < 0) end = 0;
						end = Math.min(end, scoresOrderList.size()-1);
						int count = end-start+1;
						String resp = "*" + count + "\r\n";
						System.out.println(group);
						for (int i = start; i <= end; i++) {
							KeySet keySet = scoresOrderList.get(i);
							String member = keySet.getMember();
							System.out.println("member: " + member);
							resp += "$" + member.length() + "\r\n" + member + "\r\n";
						}
						return resp;
					} catch (Exception e) {
						return "*0\r\n";
					}
				}
			} else if (cmd.equals("ZCARD")) {
				String group = command.get(1);
				synchronized (zset) {
					try {
						return ":" + zset.getScoreMap(group).size() + "\r\n";
					} catch (Exception e) {
						return ":0\r\n";
					}
				}
			} else if (cmd.equals("ZSCORE")) {
				String group = command.get(1);
				String member = command.get(2);
				synchronized (zset) {
					try {
						String score = zset.getScoreMap(group).get(member).toString();
						return "$" + score.length() + "\r\n" + score+ "\r\n";
					} catch (Exception e) {
						return "$-1\r\n";
					}
				}
			} else if (cmd.equals("ZREM")) {
				String group = command.get(1);
				String member = command.get(2);
				synchronized (zset) {
					try {
						ArrayList<KeySet> scoresOrderList = zset.getGroupOrderList(group);
						double score = zset.getScoreMap(group).get(member);
						KeySet keySet = new KeySet(member, score);
						int index = zset.getIndex(keySet, group);
						int n = scoresOrderList.size();
						while (index < n) {
							if (scoresOrderList.get(index).getMember().equals(member)) break;
							index++;
							if (scoresOrderList.get(index).getScore() > score) {
								index = -1;
								break;
							}
						}
						zset.getGroupOrderList(group).remove(index);
						zset.getScoreMap(group).remove(member);
						return ":1\r\n";
					} catch (Exception e) {
						return ":0\r\n";
					}
				}
			} else if (cmd.equals("TYPE")) {
				String key = command.get(1);
				if (redisKeys.containsKey(key)) return "+" + redisKeys.get(key) + "\r\n";
				return "+none\r\n";
			} else if (cmd.equals("INCR")) {
				String key = command.get(1);
				if (!map.containsKey(key)) {
					if (!set(command)) return null;
				}
				String[] values = map.get(key);
				String val = values[0];
				if (isInteger(val)) {
					String newVal = String.valueOf(Integer.parseInt(val)+1);
					map.put(key, new String[]{newVal, values[1]});
					return ":" + newVal + "\r\n";
				} else {
					return "-ERR value is not an integer or out of range\r\n";
				}
			} else if (cmd.equals("XADD")) {
				int n = command.size();
				String key = command.get(1);
				String id = command.get(2);
				String[] idParts = id.split("-");
				String timestamp = idParts[0];
				String idSeq = idParts.length == 2 ? idParts[1] : null;
				synchronized (streams) {
					if (id.equals("*")) {
						id = Utility.autoGenerateFullId(streams.get(key));
					} else if (idSeq != null && idSeq.equals("*")) {
						id = Utility.autoGeneratePartId(timestamp, streams.get(key));
					} else {
						id = timestamp + "." + idSeq;
					}

					if (Utility.isLessThanMin(id)) return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
					if (!Utility.validStreamID(id, streams.get(key))) {
						return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
					}

					HashMap<String, String> streamHash = new HashMap<>();
					streamHash.put("id", id);
					for (int i = 3; i < n; i += 2) {
						streamHash.put(command.get(i), command.get(i + 1));
					}
					streams.putIfAbsent(key, new Stream());
					streams.get(key).add(streamHash);

					redisKeys.put(key, "stream");

					String[] parts = id.split("\\.");
					String formattedId = parts[0] + "-" + parts[1];
					return "$" + formattedId.length() + "\r\n" + formattedId + "\r\n";
				}
			} else if (cmd.equals("XRANGE")) {
				String key = command.get(1);
				String start = command.get(2);
				String end = command.get(3);
				synchronized (streams) {
					return streams.get(key).getEntriesRange(start, end);
				}
			} else if (cmd.equals("XREAD")) {
				System.out.println("xread command: " + command);
				long timestamp = System.currentTimeMillis();
				int n = command.size();
				int i = 1;
				long blockTime = -1;
				for (; i < n; i++) {
					String arg  = command.get(i);
					if (arg.equalsIgnoreCase("streams")) break;
					if (arg.equals("block")) blockTime = Long.parseLong(command.get(i+1));
				}

				List<String> queryArgs = command.subList(i+1, n);
				int querySize = queryArgs.size();
				int keyCount = querySize/2;

				List<String> keys = new ArrayList<>();
				for (i = 0; i < keyCount; i++) {
					System.out.println("key: " + queryArgs.get(i));
					keys.add(queryArgs.get(i));
				}

				List<String> leftRanges = new ArrayList<>();
				for (int j = i; j < querySize; j++) {
					System.out.println("range: " + queryArgs.get(i));
					leftRanges.add(queryArgs.get(j));
				}

				int size = leftRanges.size();

				int[] sentinels = new int[keyCount];
				for (int k = 0; k < size; k++) {
					String key = keys.get(k);
					String leftRange = leftRanges.get(k);
					sentinels[k] = streams.get(key).getSentinelXreadBlock(leftRange);
				}

				if (blockTime >= 0) {
					String resp = "";
					int count = 0;
					for (i = 0; i < size; i++) {
						String key = keys.get(i);
						String hashResp = "*2\r\n";
						hashResp += "$" + key.length() + "\r\n" + key + "\r\n";
						hashResp += "*1\r\n" + "*2\r\n";
						String entryHash = streams.get(key).getNewEntries(sentinels[i]+1);
						if (!entryHash.isEmpty()) {
							resp += (hashResp + entryHash);
							count++;
						}
					}

					if (count > 0) {
						return "*" + count + "\r\n" + resp;
					}

					if (blockTime != 0 && System.currentTimeMillis() >= timestamp+blockTime) {
						return "*-1\r\n";
					}

					synchronized (streamWaitQueue) {
						long expTime = blockTime == 0 ? 0 : (timestamp+blockTime);
						streamWaitQueue.offer(new StreamRequest(expTime, selectKey, keys, leftRanges, sentinels));
						return "wait";
					}

				} else {
					String resp = "*" + keyCount + "\r\n";
					for (i = 0; i < size; i++) {
						String key = keys.get(i);
						resp += "*2\r\n";
						resp += "$" + key.length() + "\r\n" + key + "\r\n";
						resp += "*1\r\n" + "*2\r\n";
						String leftRange = leftRanges.get(i);
						resp += streams.get(key).getMultipleStreamsEntries(leftRange, key);
					}
					return resp;
				}
			} else if (cmd.equals("RPUSH")) {
				String key = command.get(1);

				lists.putIfAbsent(key, new LinkedList<>());
				LinkedList<ArrayList<String>> linkList = lists.get(key);
				if (linkList.isEmpty()) linkList.add(new ArrayList<>());
				ArrayList<String> lastList = linkList.getLast();

				int n = command.size();

				for (int i = 2; i < n; i++) {
					if (lastList.size() == 100) {
						lastList = new ArrayList<>();
						linkList.add(lastList);
					}
					lastList.add(command.get(i));
				}

				int entryCount = getListSize(linkList);

				synchronized (waitingQueue) {
					Queue<ClientRequest> pq = waitingQueue.getOrDefault(key, null);
					if (pq != null && !pq.isEmpty()) {
						while (!pq.isEmpty()) {
							ClientRequest cr = pq.peek();
							if (cr.getExpiryTime() != 0 && System.currentTimeMillis() >= cr.getExpiryTime()) {
								pq.poll();
								writeResponseToClient("$-1\r\n", cr.getKey());
								cr.getKey().interestOps(SelectionKey.OP_READ);
							} else {
								pq.poll();
								ArrayList<String> firstList = linkList.getFirst();
								String removed = firstList.removeFirst();
								if (firstList.isEmpty()) linkList.removeFirst();
								if (linkList.isEmpty()) lists.remove(key);
								String resp = "*2\r\n" + "$" + key.length() + "\r\n" + key + "\r\n" + "$" + removed.length() + "\r\n" + removed + "\r\n";
								writeResponseToClient(resp, cr.getKey());
								cr.getKey().interestOps(SelectionKey.OP_READ);
								break;
							}
						}
					}
				}

				return ":" + entryCount + "\r\n";

			} else if (cmd.equals("LPUSH")) {
				String key = command.get(1);
				synchronized (lists) {
					lists.putIfAbsent(key, new LinkedList<>());
					LinkedList<ArrayList<String>> linkList = lists.get(key);
					if (linkList.isEmpty()) linkList.add(new ArrayList<>());
					int n = command.size();
					int entryCount = n-2;
					ArrayList<String> entryList = new ArrayList<>(entryCount);
					for (int i = n-1; i >= 2; i--) {
						entryList.add(command.get(i));
					}
					ArrayList<String> firstList = linkList.getFirst();
					int firstListSize = firstList.size();
					if (firstListSize < 100) {
						ArrayList<String> newFirstList = new ArrayList<>();
						int remainCap = 100 - firstListSize;
						if (entryCount > remainCap) {
							newFirstList.addAll(entryList.subList(entryCount-remainCap, entryCount));
							entryCount -= remainCap;
						} else {
							newFirstList.addAll(entryList);
							entryCount = 0;
						}
						newFirstList.addAll(firstList);
						linkList.removeFirst();
						linkList.addFirst(newFirstList);
					}

					while (entryCount > 0) {
						if (entryCount > 100) {
							ArrayList<String> newEntryList = new ArrayList<>(100);
							newEntryList.addAll(entryList.subList(entryCount-100, entryCount));
							linkList.addFirst(newEntryList);
							entryCount -= 100;
						} else {
							ArrayList<String> newEntryList = new ArrayList<>(entryCount);
							newEntryList.addAll(entryList.subList(0, entryCount));
							linkList.addFirst(newEntryList);
							break;
						}
					}
					return ":" + getListSize(linkList) + "\r\n";
				}

			} else if (cmd.equals("LPOP")) {
				String key = command.get(1);
				synchronized (lists) {
					if (!lists.containsKey(key)) return "$-1\r\n";
					LinkedList<ArrayList<String>> linkList = lists.get(key);
					if (linkList.isEmpty()) return "$-1\r\n";
					int count = command.size() > 2 ? Integer.parseInt(command.get(2)) : 1;
					int entryCount = getListSize(linkList);
					count = Math.min(count, entryCount);
					int removed = 0;
					String resp = "";
					while (removed < count) {
						int remain = count - removed;
						ArrayList<String> firstList = linkList.removeFirst();
						int firstListSize = firstList.size();
						if (firstListSize <= remain) {
							for (String entry : firstList) {
								resp += "$" + entry.length() + "\r\n" + entry + "\r\n";
							}
							removed += firstListSize;
						} else {
							for (String entry : firstList) {
								resp += "$" + entry.length() + "\r\n" + entry + "\r\n";
								removed++;
								if (removed == count) break;
							}
							ArrayList<String> newEntryList = new ArrayList<>(firstListSize-remain);
							newEntryList.addAll(firstList.subList(remain, firstListSize));
							linkList.addFirst(newEntryList);
						}
					}

					return count == 1 ? resp : "*" + removed + "\r\n" + resp;
				}

			} else if (cmd.equals("BLPOP")) {
				String key = command.get(1);
				double timeout = command.size() > 2 ? Double.parseDouble(command.get(2)) : 0;
				double expireDuration = timeout * 1000;
				double expTime = timeout == 0 ? 0 : System.currentTimeMillis()+expireDuration;

				if (lists.containsKey(key)) {
					LinkedList<ArrayList<String>> linkList = lists.get(key);
					ArrayList<String> firstList = linkList.getFirst();
					String removed = firstList.removeFirst();
					if (firstList.isEmpty()) linkList.removeFirst();
					if (linkList.isEmpty()) lists.remove(key);
					return "*2\r\n" + "$" + key.length() + "\r\n" + key + "\r\n" + "$" + removed.length() + "\r\n" + removed + "\r\n";
				}

				synchronized (waitingQueue) {
					waitingQueue.putIfAbsent(key, new LinkedList<>());

					waitingQueue.get(key).offer(new ClientRequest(selectKey, (long) expTime));
				}

				selectKey.interestOps(0);

				return "wait";

			} else if (cmd.equals("LLEN")) {
				String key = command.get(1);
				if (!lists.containsKey(key)) return ":0\r\n";
				return ":" + getListSize(lists.get(key)) + "\r\n";
			} else if (cmd.equals("LRANGE")) {
				String key = command.get(1);
				if (!lists.containsKey(key)) return "*0\r\n";
				int start = Integer.parseInt(command.get(2));
				int end = Integer.parseInt(command.get(3));
				LinkedList<ArrayList<String>> linkList = lists.get(key);
				int linkSize = getListSize(linkList);
				if (start < 0) start = linkSize+start;
				if (start < 0) start = 0;
				if (end < 0) end = linkSize+end;
				if (end < 0) end = 0;
				if (start > end) return "*0\r\n";


				int currIndex = 0;
				boolean startFound = false;
				int count = 0;
				String resp = "";
				for (ArrayList<String> list : linkList) {
					if (currIndex > end) break;
					int size = list.size();
					if (!startFound) {
						if (currIndex+size > start) {
							startFound = true;
							for (int i = start - currIndex; i < size; i++) {
								String entry = list.get(i);
								resp += "$" + entry.length() + "\r\n" + entry + "\r\n";
								count++;
								currIndex++;
								if (currIndex > end) break;
							}
							continue;
						}
						currIndex += size;
					} else {
						for (int i = 0; i < size; i++) {
							String entry = list.get(i);
							resp += "$" + entry.length() + "\r\n" + entry + "\r\n";
							count++;
							currIndex++;
							if (currIndex > end) break;
						}
					}
				}
				resp = "*" + count + "\r\n" + resp;
				return resp;
			} else if (cmd.equals("GEOADD")) {
				if (command.size() < 5) throw new IllegalArgumentException("GEOADD requires at least 5 commands");
				String key = command.get(1);
				double longitude = Double.parseDouble(command.get(2));
				double latitude = Double.parseDouble(command.get(3));
				String name = command.get(4);
				long score = Encode.encode(latitude, longitude);
				if (Geo.isLongitudeValid(longitude) && Geo.isLatitudeValid(latitude)) {
					KeySet keySet = new KeySet(name, score);
					synchronized (zset) {
						zset.getMemberToScoreMap().putIfAbsent(key, new HashMap<>());
						zset.getOrderedListOfScores().putIfAbsent(key, new ArrayList<>());
						if (zset.getMemberToScoreMap().get(key).containsKey(name)) {
							double oldScore = zset.getMemberToScoreMap().get(key).get(name);
							zset.getOrderedListOfScores().get(key).remove(new KeySet(name, oldScore));
							zset.getOrderedListOfScores().get(key).add(keySet);
							zset.getOrderedListOfScores().get(key).sort(Comparator.comparingDouble(KeySet::getScore).thenComparing(KeySet::getMember));
							zset.getMemberToScoreMap().get(key).put(name, (double) score);
							return ":0\r\n";
						} else {
							zset.getMemberToScoreMap().get(key).put(name, (double) score);
							zset.getOrderedListOfScores().get(key).add(keySet);
							zset.getOrderedListOfScores().get(key).sort(Comparator.comparing(KeySet::getScore).thenComparing(KeySet::getMember));
							redisKeys.put(key, "zset");
							return ":1\r\n";
						}
					}
				}

				return "-ERR invalid longitude,latitude pair " + longitude + "," + latitude + "\r\n";

			} else if (cmd.equals("GEOPOS")) {
				String key = command.get(1);
				int n = command.size();
				int count = n-2;
				String resp = "*" + count + "\r\n";
				try {
					HashMap<String, Double> scoreMap = zset.getScoreMap(key);
					for (int i = 2; i < n; i++) {
						String place = command.get(i);
						if (!scoreMap.containsKey(place)) {
							resp += "*-1\r\n";
							continue;
						}
						double score = scoreMap.get(place);
						Decode.Coordinates coords = Decode.decode((long) score);
						String longitude = String.valueOf(coords.longitude);
						String latitude = String.valueOf(coords.latitude);
						resp += "*2\r\n";
						resp += "$" + longitude.length() + "\r\n" + longitude + "\r\n";
						resp += "$" + latitude.length() + "\r\n" + latitude + "\r\n";
					}
					return resp;
				} catch (Exception e) {
					resp = "*" + count + "\r\n";
					while (count > 0) {
						resp += "*-1\r\n";
						count--;
					}
					return resp;
				}

			} else if (cmd.equals("GEODIST")) {
				String key = command.get(1);
				String place1 = command.get(2);
				String place2 = command.get(3);
				try {
					HashMap<String, Double> scoreMap = zset.getScoreMap(key);

					double score1 = scoreMap.get(place1);
					Decode.Coordinates coords1 = Decode.decode((long) score1);
					double long1 = coords1.longitude;
					double lat1 = coords1.latitude;

					double score2 = scoreMap.get(place2);
					Decode.Coordinates coords2 = Decode.decode((long) score2);
					double long2 = coords2.longitude;
					double lat2 = coords2.latitude;

					String dist = String.valueOf(Geo.geohashGetDistance(long1, lat1, long2, lat2));

					return "$" + dist.length() + "\r\n" + dist + "\r\n";

				} catch (Exception e) {
					throw new IllegalArgumentException("GEODIST requires at least 1 coordinate pair");
				}

			} else if (cmd.equals("GET")) {
				String key = command.get(1);
				String resp;
				if (map.containsKey(key)) {
					String[] values = map.get(key);
					if (isExpired(values[1])) resp = "$" + -1 + "\r\n";
					else resp = "$" + values[0].length() + "\r\n" + values[0] + "\r\n";
				} else {
					resp = "$" + -1 + "\r\n";
				}
				return resp;
			} else if (cmd.equals("WAIT")) {

				if (master_repl_offset == 0) {
					return ":" + replicaWriters.size() + "\r\n";
				}

				int requiredAckCount = Integer.parseInt(command.get(1));
				long timeLimit = Integer.parseInt(command.get(2));
				long startTime = System.currentTimeMillis();

				int count = 0;
				for (long offset : replicaOffsets.values()) {
					if (offset >= master_repl_offset) count++;
				}

				if (timeLimit == 0 || count >= requiredAckCount) {
					return ":" + count + "\r\n";
				}

				for (SelectionKey skey : replicaWriters) {
					String ack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
					writeResponseToClient(ack, skey);
				}

				synchronized (ackWaitQueue) {
					ackWaitQueue.offer(new AckRequest(requiredAckCount, startTime+timeLimit, selectKey));
					return "wait";
				}

			} else if (cmd.equals("KEYS")) {
				if (command.size() < 2 || !command.get(1).equals("*")) {
					return "-ERR only KEYS * is supported\r\n";
				}
				synchronized (map) {
					int validKeyCount = 0;
					for (String key : map.keySet()) {
						if (!isExpired(map.get(key)[1])) {
							validKeyCount++;
						}
					}
					StringBuilder resp = new StringBuilder("*" + validKeyCount + "\r\n");
					for (String key : map.keySet()) {
						if (!isExpired(map.get(key)[1])) {
							resp.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
						}
					}
					return resp.toString();
				}
			} else {
				return null;
			}
		} catch (Exception e) {
			System.out.println("IOException: " + e.getMessage());
			return null;
		}
	}

	private static String getResp(List<String> command) {
		String resp = "*" + command.size() + "\r\n";
		for (String comp : command) {
			resp += "$" + comp.length() + "\r\n" + comp + "\r\n";
		}
		return resp;
	}

}
