public class Utility {

	public static boolean validStreamID (String id, Stream stream) {

		if (stream == null) return true;

		String[] idComps = id.split("\\.");
		long currentTimestamp = Long.parseLong(idComps[0]);
		int currentSeq = Integer.parseInt(idComps[1]);

		String[] lastIDComps = stream.getLastID().split("\\.");
		long lastTimestamp = Long.parseLong(lastIDComps[0]);
		int lastSeq = Integer.parseInt(lastIDComps[1]);

		if (currentTimestamp > lastTimestamp) return true;
		return currentTimestamp == lastTimestamp && currentSeq > lastSeq;
	}

	public static boolean isLessThanMin (String id) {
		String[] idComps = id.split("\\.");
		long currentTimestamp = Long.parseLong(idComps[0]);
		int currentSeq = Integer.parseInt(idComps[1]);
		if (currentTimestamp < 0 || currentSeq < 0) return true;
		return currentTimestamp == 0 && currentSeq == 0;
	}

	public static String autoGenerateFullId (Stream stream) {
		long timestamp = System.currentTimeMillis();
		if (stream == null) return timestamp + ".0";

		String[] lastIDComps = stream.getLastID().split("\\.");
		long lastTimestamp = Long.parseLong(lastIDComps[0]);
		if (timestamp > lastTimestamp) return timestamp + ".0";

		int lastSeq = Integer.parseInt(lastIDComps[1]);
		int nextSeq = lastSeq+1;

		if (timestamp == lastTimestamp) return timestamp + "." + nextSeq;

		return lastTimestamp + "." + nextSeq;
	}

	public static String autoGeneratePartId (String timestamp, Stream stream) {
		long stamp = Long.parseLong(timestamp);
		if (stream == null) return stamp + "." + (stamp == 0 ? 1 : 0);

		String[] lastIDComps = stream.getLastID().split("\\.");
		long lastTimestamp = Long.parseLong(lastIDComps[0]);
		int lastSeq = Integer.parseInt(lastIDComps[1]);
		int nextSeq = lastSeq+1;

		if (stamp > lastTimestamp) return stamp + ".0";
		if (stamp == lastTimestamp) return stamp + "." + nextSeq;

		return lastTimestamp + "." + nextSeq;
	}

	public static double formatIdDouble (String id) {
		String[] parts = id.split("-");
		String formatted = parts[0] + "." + parts[1];
		return Double.parseDouble(formatted);
	}

	public static String formatIdString (String id) {
		String[] parts = id.split("\\.");
		return parts[0] + "-" + parts[1];
	}


}
