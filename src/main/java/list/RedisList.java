package list;

import clients.ClientManager;

import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RedisList {

	private static final HashMap<String, LinkedList<ArrayList<String>>> lists = new HashMap<>();

	public static HashMap<String, LinkedList<ArrayList<String>>> getLists() {
		return lists;
	}

	public static String rpush (List<String> command) {
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


		ConcurrentHashMap<String, Queue<ListRequest>> listWaitQueue = ListWaitingQueueHandler.getListWaitQueue();
		synchronized (listWaitQueue) {
			Queue<ListRequest> pq = listWaitQueue.getOrDefault(key, null);
			if (pq != null && !pq.isEmpty()) {
				while (!pq.isEmpty()) {
					ListRequest cr = pq.peek();
					if (cr.getExpiryTime() != 0 && System.currentTimeMillis() >= cr.getExpiryTime()) {
						pq.poll();
						ClientManager.writeResponseToClient("$-1\r\n", cr.getKey());
						cr.getKey().interestOps(SelectionKey.OP_READ);
					} else {
						pq.poll();
						ArrayList<String> firstList = linkList.getFirst();
						String removed = firstList.removeFirst();
						if (firstList.isEmpty()) linkList.removeFirst();
						if (linkList.isEmpty()) lists.remove(key);
						String resp = "*2\r\n" + "$" + key.length() + "\r\n" + key + "\r\n" + "$" + removed.length() + "\r\n" + removed + "\r\n";
						ClientManager.writeResponseToClient(resp, cr.getKey());
						cr.getKey().interestOps(SelectionKey.OP_READ);
						break;
					}
				}
			}
		}

		return ":" + entryCount + "\r\n";

	}

	public static String lpush (List<String> command) {
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
	}

	public static String lpop (List<String> command) {
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

	}

	public static String blpop (List<String> command, SelectionKey selectKey) {
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

		ConcurrentHashMap<String, Queue<ListRequest>> listWaitQueue = ListWaitingQueueHandler.getListWaitQueue();
		synchronized (listWaitQueue) {
			listWaitQueue.putIfAbsent(key, new LinkedList<>());

			listWaitQueue.get(key).offer(new ListRequest(selectKey, (long) expTime));
		}

		selectKey.interestOps(0);

		return "wait";
	}

	public static String llen (List<String> command) {

		String key = command.get(1);
		if (!lists.containsKey(key)) return ":0\r\n";
		return ":" + getListSize(lists.get(key)) + "\r\n";
	}

	public static String lrange (List<String> command) {
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
	}

	private static int getListSize(LinkedList<ArrayList<String>> linkList) {
		int linkSize = linkList.size();
		int entryCount;
		if (linkSize == 1) entryCount = linkList.getLast().size();
		else if (linkSize == 2) entryCount = linkList.getLast().size() + linkList.getFirst().size();
		else entryCount = linkList.getFirst().size() + linkList.getLast().size() + (100*(linkSize-2));
		return entryCount;
	}
}
