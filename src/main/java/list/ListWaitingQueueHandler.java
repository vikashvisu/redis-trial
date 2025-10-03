package list;

import clients.ClientManager;

import java.nio.channels.SelectionKey;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class ListWaitingQueueHandler implements Runnable {

	private static final ConcurrentHashMap<String, Queue<ListRequest>> listWaitQueue = new ConcurrentHashMap<>();

	public static ConcurrentHashMap<String, Queue<ListRequest>> getListWaitQueue() {
		return listWaitQueue;
	}

	public void run() {
		while (true) {
			synchronized (listWaitQueue) {
				for (String key : listWaitQueue.keySet()) {
					Queue<ListRequest> q = listWaitQueue.get(key);
					if (q.isEmpty()) {
						listWaitQueue.remove(key);
						continue;
					}
					while (!q.isEmpty()) {
						ListRequest req = q.peek();
						SelectionKey skey = req.getKey();
						long exp = req.getExpiryTime();
						if (exp != 0 && System.currentTimeMillis() >= exp) {
							q.poll();
							ClientManager.writeResponseToClient("*-1\r\n", skey);
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
