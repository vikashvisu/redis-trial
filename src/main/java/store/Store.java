package store;

import server.Server;
import utility.Utility;

import java.util.HashMap;
import java.util.List;


public class Store {

	private static final HashMap<String, String[]> map = new HashMap<>();

	public static HashMap<String, String[]> getMap() {
		return map;
	}

	public static String set(List<String> command) {

		String key = command.get(1);
		String value = "0";
		if (command.size() > 2) {
			value = command.get(2);
		}
		long expTime;
		if (command.size() > 3 && command.get(3).equalsIgnoreCase("px")) {
			expTime = System.currentTimeMillis() + (long) Integer.parseInt(command.get(4));
		} else {
			expTime = -1;
		}
		String[] values = {value, String.valueOf(expTime)};
		map.put(key, values);
		Server.getRedisKeys().put(key, "string");
		return "+OK\r\n";

	}

	public static String get(List<String> command) {
		String key = command.get(1);
		String resp;
		if (map.containsKey(key)) {
			String[] values = map.get(key);
			if (Utility.isExpired(values[1])) resp = "$" + -1 + "\r\n";
			else resp = "$" + values[0].length() + "\r\n" + values[0] + "\r\n";
		} else {
			resp = "$" + -1 + "\r\n";
		}
		return resp;
	}

	public static String incr(List<String> command) {
		String key = command.get(1);
		if (!map.containsKey(key)) {
			set(command);
		}
		String[] values = map.get(key);
		String val = values[0];
		if (Utility.isInteger(val)) {
			String newVal = String.valueOf(Integer.parseInt(val)+1);
			map.put(key, new String[]{newVal, values[1]});
			return ":" + newVal + "\r\n";
		} else {
			return "-ERR value is not an integer or out of range\r\n";
		}
	}

	public static String keys(List<String> command) {
		if (command.size() < 2 || !command.get(1).equals("*")) {
			return "-ERR only KEYS * is supported\r\n";
		}
		synchronized (map) {
			int validKeyCount = 0;
			for (String key : map.keySet()) {
				if (!Utility.isExpired(map.get(key)[1])) {
					validKeyCount++;
				}
			}
			StringBuilder resp = new StringBuilder("*" + validKeyCount + "\r\n");
			for (String key : map.keySet()) {
				if (!Utility.isExpired(map.get(key)[1])) {
					resp.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
				}
			}
			return resp.toString();
		}
	}


}
