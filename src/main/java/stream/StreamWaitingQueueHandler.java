package stream;

import clients.ClientManager;

import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamWaitingQueueHandler implements Runnable {

	private static final ConcurrentLinkedQueue<StreamRequest> streamWaitQueue = new ConcurrentLinkedQueue<>();

	public void run() {
		while (true) {
			synchronized (streamWaitQueue) {
				ConcurrentLinkedQueue<StreamRequest> nextQ = new ConcurrentLinkedQueue<>();
				while (!streamWaitQueue.isEmpty()) {
					StreamRequest sr = streamWaitQueue.poll();
					long exp = sr.getExp();
					SelectionKey skey = sr.getSelectKey();
					if (exp != 0 && System.currentTimeMillis() >= exp) {
						ClientManager.writeResponseToClient("*-1\r\n", skey);
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
						String entryHash = StreamStore.getStreams().get(key).getNewEntries(sentinels[i]+1);
						if (!entryHash.isEmpty()) {
							resp += (hashResp + entryHash);
							count++;
						}
					}

					if (count > 0) {
						resp = "*" + count + "\r\n" + resp;
						ClientManager.writeResponseToClient(resp, skey);
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

	public static ConcurrentLinkedQueue<StreamRequest> getStreamWaitQueue () {
		return streamWaitQueue;
	}

}
