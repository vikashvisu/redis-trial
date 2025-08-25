import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ZSET {

	private final ConcurrentHashMap<String, HashMap<String, Double>> memberToScoreMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, ArrayList<KeySet>> orderedListOfScores = new ConcurrentHashMap<>();

	public ConcurrentHashMap<String, ArrayList<KeySet>> getOrderedListOfScores() {
		return orderedListOfScores;
	}

	public ConcurrentHashMap<String, HashMap<String, Double>> getMemberToScoreMap() {
		return memberToScoreMap;
	}

	public HashMap<String, Double> getScoreMap(String group) {
		return memberToScoreMap.get(group);
	}

	public ArrayList<KeySet> getGroupOrderList(String group) {
		return orderedListOfScores.get(group);
	}

	public int getIndex(KeySet kset, String group) {
		ArrayList<KeySet> list = getGroupOrderList(group);
		return binarySearch(0, list.size()-1, list, kset.getScore());
	}

	private int binarySearch(int left, int right, ArrayList<KeySet> list, double target) {
		if (left == right && target == list.get(left).getScore()) return left;
		if (left == right) return -1;
		int mid = (left+right)/2;
		if (list.get(mid).getScore() < target) return binarySearch(mid+1, right, list, target);
		else return binarySearch(left, mid, list, target);
	}
}
