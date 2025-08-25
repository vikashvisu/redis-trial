public class KeySet {

    private final String member;
    private final double score;

    KeySet (String member, double score) {
        this.member = member;
        this.score = score;
    }

	public String getMember() {
		return member;
	}

	public double getScore() {
		return score;
	}
}
