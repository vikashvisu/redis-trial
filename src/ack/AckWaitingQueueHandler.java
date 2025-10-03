package ack;

import clients.ClientManager;
import replication.Replication;
import server.Server;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AckWaitingQueueHandler implements Runnable {

	private static final ConcurrentLinkedQueue<AckRequest> ackWaitQueue = new ConcurrentLinkedQueue<>();

	public static ConcurrentLinkedQueue<AckRequest> getAckWaitQueue() {
		return ackWaitQueue;
	}

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
					ConcurrentHashMap<SocketChannel, Long> replicaOffsets = Replication.getReplicaOffsets();
					long master_repl_offset = Server.getMaster_repl_offset();
					for (long offset : replicaOffsets.values()) {
						if (offset >= master_repl_offset) ackCount++;
					}

					if (ackCount >= requiredAckCount || System.currentTimeMillis() >= exp) {
						String resp = ":" + ackCount + "\r\n";
						ClientManager.writeResponseToClient(resp, skey);
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
