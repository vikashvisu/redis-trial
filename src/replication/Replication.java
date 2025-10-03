package replication;

import clients.ClientManager;
import rdb.RDB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Replication {

	private static final Set<SelectionKey> replicaWriters = new HashSet<>();
	private static final ConcurrentHashMap<SocketChannel, Long> replicaOffsets = new ConcurrentHashMap<>();

	public static ConcurrentHashMap<SocketChannel, Long> getReplicaOffsets() {
		return replicaOffsets;
	}

	public static Set<SelectionKey> getReplicaWriters() {
		return replicaWriters;
	}

	public static void psync (SocketChannel client, SelectionKey key, String master_replid, long master_repl_offset) {

		try {
			String resp = "+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n";
			ClientManager.writeResponseToClient(resp, key);

			byte[] rdb = new RDB().getRDB();
			String lengthHeader = "$" + rdb.length + "\r\n";
			ClientManager.writeResponseToClient(lengthHeader, key);
			ByteBuffer bb = ByteBuffer.wrap(rdb);
			while (bb.hasRemaining()) {
				client.write(bb);
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

	}

	public static void replconf (List<String> command, SocketChannel client, SelectionKey key) {

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
			ClientManager.writeResponseToClient(resp, key);
		}
		if (command.size() > 2 && command.get(1).equalsIgnoreCase("listening-port")) {
			replicaWriters.add(key);
		}

	}

	public static void replicate (String resp) {
		for (SelectionKey skey : replicaWriters) {
			ClientManager.writeResponseToClient(resp, skey);
		}
	}

}


