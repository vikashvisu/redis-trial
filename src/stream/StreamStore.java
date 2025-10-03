package stream;

import server.Server;
import utility.Utility;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamStore {

	private static final HashMap<String, Stream> streams = new HashMap<>();

	public static HashMap<String, Stream> getStreams() {
		return streams;
	}

	public static String xadd (List<String> command) {
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

			Server.getRedisKeys().put(key, "stream");

			String[] parts = id.split("\\.");
			String formattedId = parts[0] + "-" + parts[1];
			return "$" + formattedId.length() + "\r\n" + formattedId + "\r\n";
		}
	}

	public static String xrange (List<String> command) {
		String key = command.get(1);
		String start = command.get(2);
		String end = command.get(3);
		synchronized (streams) {
			return streams.get(key).getEntriesRange(start, end);
		}
	}

	public static String xread(List<String> command, SelectionKey selectKey) {
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

			ConcurrentLinkedQueue<StreamRequest> streamWaitQueue = StreamWaitingQueueHandler.getStreamWaitQueue();
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
	}


}
