package list;

import clients.ClientManager;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ListWaitingQueueHandler implements Runnable {

	private static final ConcurrentLinkedQueue<ListRequest> listWaitQueue = new ConcurrentLinkedQueue<>();

	public static ConcurrentLinkedQueue<ListRequest> getListWaitQueue() {
		return listWaitQueue;
	}

	public void run() {
		while (true) {
			synchronized (listWaitQueue) {
				ConcurrentLinkedQueue<ListRequest> nextQ = new ConcurrentLinkedQueue<>();
				while (!listWaitQueue.isEmpty()) {
					ListRequest req = listWaitQueue.poll();
					SelectionKey skey = req.getKey();
					String listKey = req.getListKey();
					long exp = req.getExpiryTime();
					if (exp != 0 && System.currentTimeMillis() >= exp) {
						ClientManager.writeResponseToClient("*-1\r\n", skey);
						skey.interestOps(SelectionKey.OP_READ);
						continue;
					}

					LinkedList<ArrayList<String>> linkList = RedisList.getList(listKey);
					if (linkList == null || linkList.isEmpty()) {
						nextQ.offer(req);
						continue;
					}
					ArrayList<String> firstList = linkList.getFirst();
					if (firstList == null || firstList.isEmpty()) {
						nextQ.offer(req);
						continue;
					}

					String removed = firstList.removeFirst();
					if (firstList.isEmpty()) linkList.removeFirst();
					if (linkList.isEmpty()) RedisList.getLists().remove(listKey);

					String resp = "*2\r\n" + "$" + listKey.length() + "\r\n" + listKey + "\r\n" + "$" + removed.length() + "\r\n" + removed + "\r\n";
					ClientManager.writeResponseToClient(resp, skey);
					skey.interestOps(SelectionKey.OP_READ);

				}
				listWaitQueue.addAll(nextQ);

			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

}
