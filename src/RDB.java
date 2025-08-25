import java.util.HexFormat;

public class RDB {
	private static final String rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

	public RDB() {}

	public byte[] getRDB() {
		byte[] contents = HexFormat.of().parseHex(rdb);
		return contents;
	}
}
