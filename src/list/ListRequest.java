package list;

import java.nio.channels.SelectionKey;

public class ListRequest {
	private final long expiryTime;
	private final SelectionKey key;
	private final String listKey;

	ListRequest(SelectionKey key, String listKey, long expiryTime) {
		this.key = key;
		this.listKey = listKey;
		this.expiryTime = expiryTime;
	}

	public SelectionKey getKey() {
		return key;
	}

	public long getExpiryTime() {
		return expiryTime;
	}

	public String getListKey() {
		return listKey;
	}
}
