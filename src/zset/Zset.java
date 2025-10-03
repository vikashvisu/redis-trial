package zset;

import server.Server;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Zset {

	private static final ConcurrentHashMap<String, HashMap<String, Double>> memberToScoreMap = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, ArrayList<KeySet>> orderedListOfScores = new ConcurrentHashMap<>();

	public static ConcurrentHashMap<String, ArrayList<KeySet>> getOrderedListOfScores() {
		return orderedListOfScores;
	}

	public static ConcurrentHashMap<String, HashMap<String, Double>> getMemberToScoreMap() {
		return memberToScoreMap;
	}

	public static HashMap<String, Double> getScoreMap(String group) {
		return memberToScoreMap.get(group);
	}

	public static ArrayList<KeySet> getGroupOrderList(String group) {
		return orderedListOfScores.get(group);
	}

	public static int getIndex(KeySet kset, String group) {
		ArrayList<KeySet> list = getGroupOrderList(group);
		return binarySearch(0, list.size()-1, list, kset.getScore());
	}

	private static int binarySearch(int left, int right, ArrayList<KeySet> list, double target) {
		if (left == right && target == list.get(left).getScore()) return left;
		if (left == right) return -1;
		int mid = (left+right)/2;
		if (list.get(mid).getScore() < target) return binarySearch(mid+1, right, list, target);
		else return binarySearch(left, mid, list, target);
	}



	public static String zadd(List<String> command) {
		String group = command.get(1);
		double score = Double.parseDouble(command.get(2));
		String member = command.get(3);
		KeySet keySet = new KeySet(member, score);

		memberToScoreMap.putIfAbsent(group, new HashMap<>());
		orderedListOfScores.putIfAbsent(group, new ArrayList<>());
		if (memberToScoreMap.get(group).containsKey(member)) {
			double oldScore = memberToScoreMap.get(group).get(member);
			orderedListOfScores.get(group).remove(new KeySet(member, oldScore));
			orderedListOfScores.get(group).add(keySet);
			orderedListOfScores.get(group).sort(Comparator.comparingDouble(KeySet::getScore).thenComparing(KeySet::getMember));
			memberToScoreMap.get(group).put(member, score);
			return ":0\r\n";
		} else {
			memberToScoreMap.get(group).put(member, score);
			orderedListOfScores.get(group).add(keySet);
			orderedListOfScores.get(group).sort(Comparator.comparing(KeySet::getScore).thenComparing(KeySet::getMember));
			Server.getRedisKeys().put(group, "zset");
			return ":1\r\n";
		}

	}

	public static String zrank(List<String> command) {
		String group = command.get(1);
		String member = command.get(2);

		try {
			ArrayList<KeySet> scoresOrderList = getGroupOrderList(group);
			double score = getScoreMap(group).get(member);
			KeySet keySet = new KeySet(member, score);
			int rank = getIndex(keySet, group);
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

	public static String zrange(List<String> command) {
		String group = command.get(1);
		int start = Integer.parseInt(command.get(2));
		int end = Integer.parseInt(command.get(3));

		try {
			ArrayList<KeySet> scoresOrderList = getGroupOrderList(group);
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

	public static String zcard(List<String> command) {
		String group = command.get(1);

		try {
			return ":" + getScoreMap(group).size() + "\r\n";
		} catch (Exception e) {
			return ":0\r\n";
		}

	}

	public static String zscore(List<String> command) {
		String group = command.get(1);
		String member = command.get(2);

		try {
			String score = getScoreMap(group).get(member).toString();
			return "$" + score.length() + "\r\n" + score+ "\r\n";
		} catch (Exception e) {
			return "$-1\r\n";
		}

	}

	public static String zrem(List<String> command) {
		String group = command.get(1);
		String member = command.get(2);

		try {
			ArrayList<KeySet> scoresOrderList = getGroupOrderList(group);
			double score = getScoreMap(group).get(member);
			KeySet keySet = new KeySet(member, score);
			int index = getIndex(keySet, group);
			int n = scoresOrderList.size();
			while (index < n) {
				if (scoresOrderList.get(index).getMember().equals(member)) break;
				index++;
				if (scoresOrderList.get(index).getScore() > score) {
					index = -1;
					break;
				}
			}
			getGroupOrderList(group).remove(index);
			getScoreMap(group).remove(member);
			return ":1\r\n";
		} catch (Exception e) {
			return ":0\r\n";
		}

	}
}
