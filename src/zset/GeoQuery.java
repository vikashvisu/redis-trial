package zset;

import parser.Parser;
import server.Server;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class GeoQuery {

	public static String geoadd (List<String> command) {
		if (command.size() < 5) throw new IllegalArgumentException("GEOADD requires at least 5 commands");
		String key = command.get(1);
		double longitude = Double.parseDouble(command.get(2));
		double latitude = Double.parseDouble(command.get(3));
		String name = command.get(4);
		long score = Encode.encode(latitude, longitude);
		if (Geo.isLongitudeValid(longitude) && Geo.isLatitudeValid(latitude)) {
			KeySet keySet = new KeySet(name, score);

			Zset.getMemberToScoreMap().putIfAbsent(key, new HashMap<>());
			Zset.getOrderedListOfScores().putIfAbsent(key, new ArrayList<>());
			if (Zset.getMemberToScoreMap().get(key).containsKey(name)) {
				double oldScore = Zset.getMemberToScoreMap().get(key).get(name);
				Zset.getOrderedListOfScores().get(key).remove(new KeySet(name, oldScore));
				Zset.getOrderedListOfScores().get(key).add(keySet);
				Zset.getOrderedListOfScores().get(key).sort(Comparator.comparingDouble(KeySet::getScore).thenComparing(KeySet::getMember));
				Zset.getMemberToScoreMap().get(key).put(name, (double) score);
				return ":0\r\n";
			} else {
				Zset.getMemberToScoreMap().get(key).put(name, (double) score);
				Zset.getOrderedListOfScores().get(key).add(keySet);
				Zset.getOrderedListOfScores().get(key).sort(Comparator.comparing(KeySet::getScore).thenComparing(KeySet::getMember));
				Server.getRedisKeys().put(key, "Zset");
				return ":1\r\n";
			}

		}

		return "-ERR invalid longitude,latitude pair " + longitude + "," + latitude + "\r\n";

	}

	public static String geopos(List<String> command) {
		String key = command.get(1);
		int n = command.size();
		int count = n-2;
		String resp = "*" + count + "\r\n";
		try {
			HashMap<String, Double> scoreMap = Zset.getScoreMap(key);
			for (int i = 2; i < n; i++) {
				String place = command.get(i);
				if (!scoreMap.containsKey(place)) {
					resp += "*-1\r\n";
					continue;
				}
				double score = scoreMap.get(place);
				Decode.Coordinates coords = Decode.decode((long) score);
				String longitude = String.valueOf(coords.longitude);
				String latitude = String.valueOf(coords.latitude);
				resp += "*2\r\n";
				resp += "$" + longitude.length() + "\r\n" + longitude + "\r\n";
				resp += "$" + latitude.length() + "\r\n" + latitude + "\r\n";
			}
			return resp;
		} catch (Exception e) {
			resp = "*" + count + "\r\n";
			while (count > 0) {
				resp += "*-1\r\n";
				count--;
			}
			return resp;
		}
	}

	public static String geodist(List<String> command) {
		String key = command.get(1);
		String place1 = command.get(2);
		String place2 = command.get(3);
		try {
			HashMap<String, Double> scoreMap = Zset.getScoreMap(key);

			double score1 = scoreMap.get(place1);
			double score2 = scoreMap.get(place2);

			String dist = String.valueOf(getDistance(score1, score2));

			return "$" + dist.length() + "\r\n" + dist + "\r\n";

		} catch (Exception e) {
			throw new IllegalArgumentException("GEODIST requires at least 1 coordinate pair");
		}
	}

	private static double getDistance(double score1, double score2) {
		Decode.Coordinates coords1 = Decode.decode((long) score1);
		double long1 = coords1.longitude;
		double lat1 = coords1.latitude;

		Decode.Coordinates coords2 = Decode.decode((long) score2);
		double long2 = coords2.longitude;
		double lat2 = coords2.latitude;

		return Geo.geohashGetDistance(long1, lat1, long2, lat2);
	}

	public static String geosearch (List<String> command) {
		String group = command.get(1);

		if (command.size() == 8 && command.get(2).equalsIgnoreCase("FROMLONLAT")) {
			double longitude = Double.parseDouble(command.get(3));
			double latitude = Double.parseDouble(command.get(4));
			double locScore = Encode.encode(latitude, longitude);

			double distance = Double.parseDouble(command.get(6));

			ArrayList<KeySet> orderList = Zset.getGroupOrderList(group);

			List<String> places = new ArrayList<>();

			for (KeySet set : orderList) {
				double result = getDistance(locScore, set.getScore());
				if (result <= distance) {
					places.add(set.getMember());
				}
			}

			return Parser.getResp(places);
		}
		return "*0\r\n";
	}
}
