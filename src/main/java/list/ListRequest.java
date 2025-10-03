package list;

import java.nio.channels.SelectionKey;

public class ListRequest {
	private final long expiryTime;
	private final SelectionKey key;

	ListRequest(SelectionKey key, long expiryTime) {
		this.key = key;
		this.expiryTime = expiryTime;
	}

	public SelectionKey getKey() {
		return key;
	}

	public long getExpiryTime() {
		return expiryTime;
	}
}
