import java.nio.channels.SelectionKey;

public class AckRequest {

	private final int requiredCount;
	private final long exp;
	private final SelectionKey skey;

	AckRequest(int requiredCount, long exp, SelectionKey skey) {
		this.requiredCount = requiredCount;
		this.exp = exp;
		this.skey = skey;
	}

	public int getRequiredCount() {
		return requiredCount;
	}

	public long getExp() {
		return exp;
	}

	public SelectionKey getSkey() {
		return skey;
	}
}
