package server;

import ack.Ack;
import ack.AckWaitingQueueHandler;
import clients.ClientManager;
import constant.CHECK;
import constant.OPERATION;
import constant.REPLICA;
import execute.CommandExecutor;
import list.ListWaitingQueueHandler;
import multi.MultiExecutor;
import parser.Parser;
import rdb.RDB;
import replication.MasterConnectionHandler;
import replication.Replication;
import store.Store;
import stream.StreamWaitingQueueHandler;
import subscription.Subscription;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Server {

	private static final Set<String> subModeCmds = new HashSet<>(Arrays.asList("SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"));
	private static Set<SocketChannel> clients;
	private static final HashMap<String, String> redisKeys = new HashMap<>();
	protected static int port;
	private static final Set<String> writeCmds = new HashSet<>(Arrays.asList("SET", "INCR"));
	private static Selector selector;

	private static long master_repl_offset = 0;
	private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	private static String dir;
	private static String dbfilename;

	private static String masterHost;
	private static int masterPort;
	private static boolean isReplica = false;


	public Server (int port, String dir, String dbfilename) {
		Server.port = port;
		Server.dir = dir;
		Server.dbfilename = dbfilename;

		if (dir != null && dbfilename != null) {
			RDB.loadRDB(dir, dbfilename, Store.getMap());
		}
	}

	public Server (String masterHost, int masterPort, int replicaPort) {
		Server.port = replicaPort;
		Server.masterHost = masterHost;
		Server.masterPort = masterPort;
		isReplica = true;
	}

	public static HashMap<String, String> getRedisKeys() {
		return redisKeys;
	}

	public static long getMaster_repl_offset() {
		return master_repl_offset;
	}

	public void register () {
		try {
			selector = Selector.open();
			ServerSocketChannel ssc = ServerSocketChannel.open();
			ssc.configureBlocking(false);
			ssc.bind(new InetSocketAddress(port));
			ssc.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}
	}


	public void start () {

		try {

			if (isReplica) {
				new Thread(new MasterConnectionHandler(masterHost, masterPort, port)).start();
			}

			new Thread(new ListWaitingQueueHandler()).start();
			new Thread(new StreamWaitingQueueHandler()).start();
			new Thread(new AckWaitingQueueHandler()).start();

			clients = ClientManager.getClients();

			// Start listening to connections
			while (true) {
				try {
					selector.select();
					Set<SelectionKey> keys = selector.selectedKeys();
					Iterator<SelectionKey> it = keys.iterator();
					while (it.hasNext()) {
						SelectionKey key = it.next();
						if (key.isAcceptable()) {
							acceptConnection(key);
						} else if (key.isReadable()) {
							readCommand(key);
						}
						it.remove();
					}
				} catch (IOException e) {
					throw new RuntimeException("-ERR connection error");
				}
			}

		} catch (RuntimeException e) {
			System.out.println("RuntimeException: " + e.getMessage());
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

	private static void acceptConnection(SelectionKey key) {
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
					List<String> command = Parser.parseRESP(sb);
					System.out.println("command being read: " + command);
					if (command == null || command.isEmpty()) break;
					String cmd = command.getFirst().toUpperCase();

					System.out.println("command next: " + command);

					if (Subscription.isSubMode(key) && !subModeCmds.contains(cmd)) {
						String resp = "-ERR Can't execute '" + cmd + "' in subscribed mode\r\n";
						ClientManager.writeResponseToClient(resp, key);
						continue;
					}

					if (cmd.equals(REPLICA.PSYNC)) {
						Replication.psync(client, key, master_replid, master_repl_offset);
					} else if (cmd.equals(REPLICA.REPLCONF)) {
						Replication.replconf(command, client, key);
					} else if (cmd.equals(OPERATION.EXEC)) {
						MultiExecutor.exec(command, key, client);
					} else if (cmd.equals(OPERATION.DISCARD)) {
						MultiExecutor.discard(key);
					} else if (MultiExecutor.isMultiModeOn(key)) {
						String resp = MultiExecutor.addToQueue(command, key);
						ClientManager.writeResponseToClient(resp, key);
					} else if (cmd.equals(CHECK.WAIT)) {
						System.out.println("calls ack wait method from main");
						String resp = Ack.wait(command, key, master_repl_offset);
						if (resp.equals("wait")) continue;
						ClientManager.writeResponseToClient(resp, key);
					} else {
						String resp = CommandExecutor.execute(command, client, key);
						System.out.println("result: " + resp);
						if (resp == null) throw new IOException("unknown command: " + command);
						if (resp.equals("wait")) continue;
						ClientManager.writeResponseToClient(resp, key);
					}

					if (writeCmds.contains(cmd)) {
						String resp = Parser.getResp(command);
						master_repl_offset += resp.getBytes().length;
						Replication.replicate(resp);
					}

				}
			}

		} catch (IOException e) {
			try	{
				Replication.getReplicaWriters().remove(key);
				Replication.getReplicaOffsets().remove(client);

				clients.remove(client);
				key.cancel();
				client.close();
			} catch (IOException ignored) {}
			System.out.println("IOException: " + e.getMessage());
		}
	}

	public static String ping (List<String> command, SocketChannel s, SelectionKey selectKey) {
		if (s != null && Subscription.isSubMode(selectKey)) {
			String payload = command.size() > 1 ? command.get(1) : null;
			String resp = "*2" + "\r\n" + "$4\r\n" + "pong" + "\r\n";
			resp += payload == null ? "$0\r\n\r\n" : "$" + payload.length() + "\r\n" + payload + "\r\n";
			return resp;
		}
		return "+PONG\r\n";
	}

	public static String echo (List<String> command) {
		String arg = command.get(1);
		return "$" + arg.length() + "\r\n" + arg + "\r\n";
	}

	public static String type (List<String> command) {
		String key = command.get(1);
		if (redisKeys.containsKey(key)) return "+" + redisKeys.get(key) + "\r\n";
		return "+none\r\n";
	}

	public static String info (List<String> command) {
		if (command.get(1).equalsIgnoreCase("replication")) {
			String role = isReplica ? "slave" : "master";
			String infoRepl = "+role:" + role + "\r\n" + "+master_replid:" + master_replid + "\r\n" + "+master_repl_offset:" + master_repl_offset + "\r\n";
			return "$" + infoRepl.length() + "\r\n" + infoRepl + "\r\n";
		}
		return null;
	}

	public static String config (List<String> command) {
		if (command.size() == 3 && command.get(1).equalsIgnoreCase("get") && command.get(2).equalsIgnoreCase("dir")) {
			String dirValue = dir != null ? dir : "";
			return "*2\r\n$3\r\ndir\r\n$" + dirValue.length() + "\r\n" + dirValue + "\r\n";
		}
		return null;
	}


}
