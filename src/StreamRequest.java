import java.nio.channels.SelectionKey;
import java.util.List;

public class StreamRequest {

	private final long exp;
	private final SelectionKey selectKey;
	private final List<String> streamKeys;
	private final List<String> leftRanges;
	private final int size;
	private final int[] sentinels;

	public StreamRequest(long exp, SelectionKey selectKey, List<String> streamKeys, List<String> leftRanges, int[] sentinels) {
		this.exp = exp;
		this.selectKey = selectKey;
		this.streamKeys = streamKeys;
		this.leftRanges = leftRanges;
		this.size = leftRanges.size();
		this.sentinels = sentinels;
	}

	public int getSize() {
		return size;
	}

	public List<String> getLeftRanges() {
		return leftRanges;
	}

	public List<String> getStreamKeys() {
		return streamKeys;
	}

	public long getExp() {
		return exp;
	}

	public SelectionKey getSelectKey() {
		return selectKey;
	}

	public int[] getSentinels() {
		return sentinels;
	}
}
