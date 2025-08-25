import java.util.ArrayList;
import java.util.HashMap;

public class Stream {

	private final ArrayList<HashMap<String, String>> hashList = new ArrayList<>();

	public void add (HashMap<String, String> hash) {
		hashList.add(hash);
	}

	public String getLastID () {
		return hashList.getLast().get("id");
	}

	public boolean isEmpty () {
		return hashList.isEmpty();
	}

	public String getEntriesRange(String start, String end) {
		int n = hashList.size();
		int startIndex = start.equals("-") ? 0 : binarySearchStartIndex(Utility.formatIdDouble(start), 0, n-1);
		int endIndex;
		if (end.equals("+")) {
			endIndex = n-1;
		} else if (start.equals(end)) {
			endIndex = startIndex;
		} else endIndex = binarySearchEndIndex(Utility.formatIdDouble(end), 0, n-1);

		System.out.println("startIndex: " + startIndex);
		System.out.println("endIndex: " + endIndex);

		int count = endIndex-startIndex+1;
		String resp = "*" + count + "\r\n";
		for  (int i = startIndex; i <= endIndex; i++) {
			resp += "*2\r\n";
			HashMap<String, String> hash = hashList.get(i);
			String id = Utility.formatIdString(hash.get("id"));
			resp += "$" + id.length() + "\r\n" + id + "\r\n";
			int contentLength = (hash.size()*2)-2;
			resp += "*" + contentLength + "\r\n";
			for (String key : hash.keySet()) {
				if (key.equals("id")) continue;
				String value = hash.get(key);
				resp += "$" + key.length() + "\r\n" + key + "\r\n";
				resp += "$" + value.length() + "\r\n" + value + "\r\n";
			}
		}
		return resp;
	}

	public String getMultipleStreamsEntries(String start, String key) {
		int n = hashList.size();
		int startIndex = start.equals("-") ? 0 : binarySearchStartIndex(Utility.formatIdDouble(start), 0, n-1);
		String resp = "";
		for  (int i = startIndex; i < n; i++) {
			HashMap<String, String> hash = hashList.get(i);
			String id = Utility.formatIdString(hash.get("id"));
			resp += "$" + id.length() + "\r\n" + id + "\r\n";
			int contentLength = (hash.size()*2)-2;
			resp += "*" + contentLength + "\r\n";
			for (String k : hash.keySet()) {
				if (k.equals("id")) continue;
				String value = hash.get(k);
				resp += "$" + k.length() + "\r\n" + k + "\r\n";
				resp += "$" + value.length() + "\r\n" + value + "\r\n";
			}
		}
		return resp;
	}

	public int getSentinelXreadBlock(String start) {
		int n = hashList.size();
		if (start.equals("$")) return n-1;
		return binarySearchStartIndex(Utility.formatIdDouble(start), 0, n-1);
	}

	public String getNewEntries (int start, int count) {
		int n = hashList.size();
		int end = count == -1 ? n : start+count;
		if (start >= n || end > n) return "";
		System.out.println("start: " + start);
		System.out.println("end: " + end);
		System.out.println("n: " + n);
		String resp = "";
		for (int i = start; i < end; i++) {
			HashMap<String, String> hash = hashList.get(i);
			String id = Utility.formatIdString(hash.get("id"));
			resp += "$" + id.length() + "\r\n" + id + "\r\n";
			int contentLength = (hash.size()*2)-2;
			resp += "*" + contentLength + "\r\n";
			for (String k : hash.keySet()) {
				if (k.equals("id")) continue;
				String value = hash.get(k);
				resp += "$" + k.length() + "\r\n" + k + "\r\n";
				resp += "$" + value.length() + "\r\n" + value + "\r\n";
			}
		}
		return resp;
	}

	private int binarySearchEndIndex (double target, int left, int right) {

		if (left == right) {
			double leftVal = Double.parseDouble(hashList.get(left).get("id"));
			if (leftVal <= target) return left;
			else return -1;
		}

		int mid = (left+right)/2;
		double midVal = Double.parseDouble(hashList.get(mid).get("id"));
		if (midVal > target) return binarySearchStartIndex(target, left, mid-1);
		else if (midVal < target) return binarySearchStartIndex(target, mid, right);
		else return mid;
	}

	private int binarySearchStartIndex (double target, int left, int right) {

		if (left == right) {
			double leftVal = Double.parseDouble(hashList.get(left).get("id"));
			if (leftVal >= target) return left;
			else return -1;
		}

		int mid = (left+right)/2;
		double midVal = Double.parseDouble(hashList.get(mid).get("id"));
		if (midVal > target) return binarySearchStartIndex(target, left, mid);
		else if (midVal < target) return binarySearchStartIndex(target, mid+1, right);
		else return mid;
	}

}
