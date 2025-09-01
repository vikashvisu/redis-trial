public class Geo {
	private final double longitude;
	private final double latitude;
	private final String name;
	private static final double MAX_LONGITUDE = 180;
	private static final double MIN_LONGITUDE = -180;
	private static final double MAX_LATITUDE = 85.05112878;
	private static final double MIN_LATITUDE = -85.05112878;
	private static final double EARTH_RADIUS_IN_METERS = 6372797.560856;

	public Geo(double longitude, double latitude, String name) {
		this.longitude = longitude;
		this.latitude = latitude;
		this.name = name;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public String getName() {
		return name;
	}

	public static boolean isLongitudeValid(double longitude) {
		return longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
	}

	public static boolean isLatitudeValid(double latitude) {
		return latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE;
	}

	// Convert degrees to radians
	private static double degRad(double degrees) {
		return degrees * Math.PI / 180.0;
	}

	// Calculate distance between two points given their latitude and longitude
	public static double geohashGetDistance(double lon1d, double lat1d, double lon2d, double lat2d) {
		double lat1r, lon1r, lat2r, lon2r, u, v, a;
		lon1r = degRad(lon1d);
		lon2r = degRad(lon2d);
		v = Math.sin((lon2r - lon1r) / 2);
		/* if v == 0 we can avoid doing expensive math when lons are practically the same */
		if (v == 0.0) {
			return geohashGetLatDistance(lat1d, lat2d);
		}
		lat1r = degRad(lat1d);
		lat2r = degRad(lat2d);
		u = Math.sin((lat2r - lat1r) / 2);
		a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v;
		return 2.0 * EARTH_RADIUS_IN_METERS * Math.asin(Math.sqrt(a));
	}

	// Calculate latitudinal distance (used when longitudes are the same)
	private static double geohashGetLatDistance(double lat1d, double lat2d) {
		double lat1r = degRad(lat1d);
		double lat2r = degRad(lat2d);
		double u = Math.sin((lat2r - lat1r) / 2);
		return 2.0 * EARTH_RADIUS_IN_METERS * Math.asin(Math.sqrt(u * u));
	}
}
