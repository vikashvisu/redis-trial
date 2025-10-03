package ack;

import clients.ClientManager;
import replication.Replication;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Ack {

	public static String wait (List<String> command, SelectionKey selectKey, long master_repl_offset) {

		System.out.println("wait command: " + command);
		ConcurrentHashMap<SocketChannel, Long> replicaOffsets = Replication.getReplicaOffsets();
		Set<SelectionKey> replicaWriters = Replication.getReplicaWriters();

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
			ClientManager.writeResponseToClient(ack, skey);
			System.out.println("sending ack to replicas");
		}

		System.out.println("coming here before the ack queue handler");

		ConcurrentLinkedQueue<AckRequest> ackWaitQueue = AckWaitingQueueHandler.getAckWaitQueue();
		synchronized (ackWaitQueue) {
			ackWaitQueue.offer(new AckRequest(requiredAckCount, startTime+timeLimit, selectKey));
			System.out.println("added the ack request");
			return "wait";
		}
	}

}
