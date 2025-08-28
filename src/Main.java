import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
	private static final ConcurrentHashMap<String, String[]> map = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> redisKeys = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, LinkedList<ArrayList<String>>> lists = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Queue<PopTicket>> popQueue = new ConcurrentHashMap<>();
	private static final ZSET zset = new ZSET();
	private static String masterHost;
	private static int masterPort;
	private static int replicaPort;
	private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	private static long master_repl_offset = 0;
	private static long replica_repl_offset = 0;
	private static boolean isReplica = false;
	private static final Set<String> writeCmds = new HashSet<>(Arrays.asList("SET", "INCR"));
	private static final Set<BufferedWriter> replicaWriters = Collections.synchronizedSet(new HashSet<>());
	private static final ConcurrentHashMap<Socket, Long> replicaOffsets = new ConcurrentHashMap<>();
	private static String dir;
	private static String dbfilename;
	private static final ConcurrentHashMap<String, Set<Socket>> channelToSubs = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<Socket, Set<String>> subToChannels = new ConcurrentHashMap<>();
	private static final Set<String> subModeCmds = new HashSet<>(Arrays.asList("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"));

	public static void main(String[] args) {
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		System.out.println("Logs from your program will appear here!");

		ServerSocket serverSocket = null;
		Socket clientSocket = null;
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

			if (isReplica) {
				new Thread(new MasterConnectionHandler(masterHost, masterPort)).start();
			}

			serverSocket = new ServerSocket(port);
			// Since the tester restarts your program quite often, setting SO_REUSEADDR
			// ensures that we don't run into 'Address already in use' errors
			serverSocket.setReuseAddress(true);

			// Handle multiple connections
			while(true){
				clientSocket = serverSocket.accept();
				new Thread(new ClientHandler(clientSocket, isReplica)).start();
			}
		} catch (IOException e) {
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
						execCommand(command, null);
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

	private static boolean isSubMode(Socket s) {
		return subToChannels.containsKey(s);
	}

	private static int getListSize(LinkedList<ArrayList<String>> linkList) {
		int linkSize = linkList.size();
		int entryCount;
		if (linkSize == 1) entryCount = linkList.getLast().size();
		else if (linkSize == 2) entryCount = linkList.getLast().size() + linkList.getFirst().size();
		else entryCount = linkList.getFirst().size() + linkList.getLast().size() + (100*(linkSize-2));
		return entryCount;
	}

	private static String execCommand(List<String> command, Socket s) {

		try {
			String cmd = command.get(0).toUpperCase();
			if (cmd.equals("PUBLISH")) {
				String channel = command.get(1);
				String msg = command.get(2);
				int subCount = 0;
				synchronized (channelToSubs) {
					Set<Socket> channels = channelToSubs.get(channel);
					String resp = "*3\r\n$7\r\nmessage\r\n";
					resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
					resp += "$" + msg.length() + "\r\n" + msg + "\r\n";
					for  (Socket socket : channels) {
						OutputStream os = socket.getOutputStream();
						os.write(resp.getBytes());
						os.flush();
					}
					subCount = channelToSubs.get(channel).size();
				}
				return ":" + subCount + "\r\n";
			} else if (cmd.equals("PING")) {
				if (s != null && isSubMode(s)) {
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

				if (blockTime == 0) {
					String resp = "";
					int count = 0;
					while (count == 0) {
						synchronized (streams) {
							for (i = 0; i < size; i++) {
								String key = keys.get(i);
								String hashResp = "*2\r\n";
								hashResp += "$" + key.length() + "\r\n" + key + "\r\n";
								hashResp += "*1\r\n" + "*2\r\n";
								System.out.println("sentinel: " + sentinels[i]);
								String entryHash = streams.get(key).getNewEntries(sentinels[i]+1, 1);
								if (!entryHash.isEmpty()) {
									resp = "*1\r\n";
									resp += (hashResp + entryHash);
									count++;
									break;
								}
							}
						}
						if (count == 1) break;
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					return resp;
				} else if (blockTime > 0) {
					String resp = "";
					while (System.currentTimeMillis() < timestamp + blockTime) {
						resp = "";
						synchronized (streams) {
							int count = 0;
							for (i = 0; i < size; i++) {
								String key = keys.get(i);
								String hashResp = "*2\r\n";
								hashResp += "$" + key.length() + "\r\n" + key + "\r\n";
								hashResp += "*1\r\n" + "*2\r\n";
								String entryHash = streams.get(key).getNewEntries(sentinels[i]+1, -1);
								if (!entryHash.isEmpty()) {
									resp += (hashResp + entryHash);
									count++;
								}
							}
							if (count > 0) resp = "*" + count + "\r\n" + resp;
						}
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
					return !resp.isEmpty() ? resp : "$-1\r\n";
				} else {
					synchronized (streams) {
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
				}
			} else if (cmd.equals("RPUSH")) {
				String key = command.get(1);
				synchronized (lists) {
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

					System.out.println("lock is released");
					return ":" + getListSize(linkList) + "\r\n";

				}
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
				Queue<PopTicket> q = popQueue.get(key);
				while (true) {
					if (q.peek().getSocket() == s) break;
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				while (true) {
					if (q.peek().getExpTime() != 0 && System.currentTimeMillis() >= q.peek().getExpTime()) return "$-1\r\n";

					synchronized (lists) {
						try {
							if (lists.containsKey(key)) {
								LinkedList<ArrayList<String>> linkList = lists.get(key);
								ArrayList<String> firstList = linkList.getFirst();
								String removed = firstList.removeFirst();
								if (firstList.isEmpty()) linkList.removeFirst();
								if (linkList.isEmpty()) lists.remove(key);
								q.poll();
								System.out.println("address: " + s.getRemoteSocketAddress());
								System.out.println("removed: " + removed);
								return "*2\r\n" + "$" + key.length() + "\r\n" + key + "\r\n" + "$" + removed.length() + "\r\n" + removed + "\r\n";
							}
						} catch (RuntimeException e) {
							System.out.println("IOException: " + e.getMessage());
						}
					}

					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}

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

				for (BufferedWriter replicaOut : replicaWriters) {
					String ack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
					replicaOut.write(ack);
					replicaOut.flush();
				}

				int requiredAckCount = Integer.parseInt(command.get(1));
				int timeLimit = Integer.parseInt(command.get(2));
				long startTime = System.currentTimeMillis();
				int ackCount = 0;

				if (master_repl_offset == 0) {
					synchronized (replicaWriters) {
						ackCount = replicaWriters.size();
					}
					return ":" + ackCount + "\r\n";
				}

				while (System.currentTimeMillis() - startTime < timeLimit) {
					synchronized (replicaOffsets) {
						ackCount = 0;
						for (long offset : replicaOffsets.values()) {
							if (offset >= master_repl_offset) ackCount++;
						}
						if (ackCount >= requiredAckCount) break;
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				return ":" + ackCount + "\r\n";
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

	static class ClientHandler implements Runnable {
		private Socket socket;
		private MultiHandler multiHandler;
		private boolean isReplica;
		private BufferedWriter out;
		public ClientHandler(Socket socket, boolean isReplica) {
			this.socket = socket;
			this.multiHandler = new MultiHandler();
			this.isReplica = isReplica;
		}

		public void run() {
			try {
				InputStream in = socket.getInputStream();
				out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				OutputStream os = socket.getOutputStream();
				while (true) {
					List<String> command = parseRESP(in);
					if (command != null && !command.isEmpty()) {
						String cmd = command.get(0).toUpperCase();

						if (isSubMode(socket) && !subModeCmds.contains(cmd)) {
							String resp = "-ERR Can't execute '" + cmd + "' in subscribed mode\r\n";
							out.write(resp);
							out.flush();
							continue;
						}

						if (cmd.equals("BLPOP")) {
							synchronized (popQueue) {
								String key = command.get(1);
								double timeout = command.size() > 2 ? Double.parseDouble(command.get(2)) : 0;
								double expireDuration = timeout * 1000;
								double expTime = timeout == 0 ? 0 : System.currentTimeMillis()+expireDuration;
								popQueue.putIfAbsent(key, new LinkedList<>());
								popQueue.get(key).offer(new PopTicket(socket, expTime));
							}
						}


						if  (cmd.equals("UNSUBSCRIBE")) {
							String channel = command.get(1);
							synchronized (channelToSubs) {
								if (channelToSubs.containsKey(channel)) {
									channelToSubs.get(channel).remove(socket);
									if  (channelToSubs.get(channel).isEmpty()) {
										channelToSubs.remove(channel);
									}
								}
							}
							synchronized (subToChannels) {
								if (subToChannels.containsKey(socket)) {
									subToChannels.get(socket).remove(channel);
									if  (subToChannels.get(socket).isEmpty()) {
										subToChannels.remove(socket);
									}
								}
							}
							int channelCount = subToChannels.containsKey(socket) ? subToChannels.get(socket).size() : 0;
							String resp = "*3"+ "\r\n" + "$11" + "\r\n" + "unsubscribe" + "\r\n";
							resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
							resp += ":" + channelCount + "\r\n";
							out.write(resp);
							out.flush();
						} else if  (cmd.equals("SUBSCRIBE")) {
							String channel = command.get(1);
							synchronized (channelToSubs) {
								channelToSubs.putIfAbsent(channel, new HashSet<>());
								channelToSubs.get(channel).add(socket);
							}
							synchronized (subToChannels) {
								subToChannels.putIfAbsent(socket, new HashSet<>());
								subToChannels.get(socket).add(channel);
							}
							int channelCount = subToChannels.get(socket).size();
							String resp = "*3"+ "\r\n" + "$9" + "\r\n" + "subscribe" + "\r\n";
							resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
							resp += ":" + channelCount + "\r\n";
							out.write(resp);
							out.flush();
						} else if (cmd.equals("CONFIG")) {
							if (command.size() == 3 && command.get(1).equalsIgnoreCase("get") && command.get(2).equalsIgnoreCase("dir")) {
								String dirValue = dir != null ? dir : "";
								String resp = "*2\r\n$3\r\ndir\r\n$" + dirValue.length() + "\r\n" + dirValue + "\r\n";
								out.write(resp);
								out.flush();
							}
						} else if (cmd.equals("PSYNC")) {
							out.write("+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n");
							out.flush();

							byte[] rdb = new RDB().getRDB();
							os.write(("$" + rdb.length + "\r\n").getBytes());
							os.write(rdb);
							os.flush();
						} else if (cmd.equals("REPLCONF")) {
							boolean sendOk = true;
							if (command.size() > 2 && command.get(1).toUpperCase().equals("ACK")) {
								long offset = Long.parseLong(command.get(2));
								replicaOffsets.put(socket, offset);
								sendOk = false;
							}
							if (sendOk) {
								out.write("+OK\r\n");
								out.flush();
							}
							if (command.size() > 2 && command.get(1).equalsIgnoreCase("listening-port")) {
								replicaWriters.add(out);
							}
						} else if (cmd.equals("INFO")) {
							if (command.get(1).equalsIgnoreCase("replication")) {
								String role = isReplica ? "slave" : "master";
								String infoRepl = "+role:" + role + "\r\n" + "+master_replid:" + master_replid + "\r\n" + "+master_repl_offset:" + master_repl_offset + "\r\n";
								String resp = "$" + infoRepl.length() + "\r\n" + infoRepl + "\r\n";
								socket.getOutputStream().write(resp.getBytes());
								socket.getOutputStream().flush();
							}
						} else if (cmd.equals("MULTI")) {
							multiHandler.init();
							out.write("+OK\r\n");
							out.flush();
						} else if (cmd.equals("DISCARD")) {
							if (!multiHandler.isOn()) {
								out.write("-ERR DISCARD without MULTI\r\n");
							} else {
								multiHandler.clear();
								out.write("+OK\r\n");
							}
							out.flush();
						} else if (cmd.equals("EXEC")) {
							if (!multiHandler.isOn()) {
								out.write("-ERR EXEC without MULTI\r\n");
								out.flush();
							} else if (multiHandler.isEmpty()) {
								multiHandler.clear();
								out.write("*0\r\n");
								out.flush();
							} else {
								out.write("*" + multiHandler.getSize() + "\r\n");
								while (!multiHandler.isEmpty()) {
									List<String> args = multiHandler.getNext();
									String msg = execCommand(args, socket);
									if (msg == null) {
										throw new IOException("unknown command: " + command);
									} else {
										out.write(msg);
									}
								}
								out.flush();
								multiHandler.clear();
							}
						} else {
							if (multiHandler.isOn()) {
								multiHandler.add(command);
								out.write("+QUEUED\r\n");
								out.flush();
							} else {
								String msg = execCommand(command, socket);
								if (msg == null) throw new IOException("unknown command: " + command);
								out.write(msg);
								out.flush();
							}
						}
						if (writeCmds.contains(cmd)) {
							String resp = getResp(command);
							master_repl_offset += resp.getBytes().length;
							for (BufferedWriter replicaOut : replicaWriters) {
								replicaOut.write(resp);
								replicaOut.flush();
							}
						}
					}
				}
			} catch (IOException e) {
				System.out.println("IOException: " + e.getMessage());
			} finally {
				replicaWriters.remove(out);
				replicaOffsets.remove(socket);
				try {
					if (socket != null) {
						socket.close();
					}
				} catch (IOException e) {
					System.out.println("IOException: " + e.getMessage());
				}
			}
		}
	}
}
