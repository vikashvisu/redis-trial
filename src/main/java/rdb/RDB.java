package rdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HexFormat;

public class RDB {
	private static final String rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

	public RDB() {}

	public byte[] getRDB() {
		byte[] contents = HexFormat.of().parseHex(rdb);
		return contents;
	}

	public static void loadRDB(String dir, String dbfilename, HashMap<String, String[]> map) {
		File file = new File(dir, dbfilename);
		if (!file.exists()) {
			System.out.println("rdb.RDB file does not exist: " + file.getPath());
			return;
		}
		try (FileInputStream fis = new FileInputStream(file)) {
			byte[] header = new byte[9];
			if (fis.read(header) != 9 || !new String(header).equals("REDIS0011")) {
				throw new IOException("Invalid rdb.RDB header");
			}
			System.out.println("Header read: " + new String(header));
			while (true) {
				int opcode = fis.read();
				if (opcode == -1 || opcode == 0xFF) {
					System.out.println("Reached EOF or FF, breaking");
					break;
				}
				System.out.println("Opcode: 0x" + Integer.toHexString(opcode));
				if (opcode == 0xFA) {
					System.out.println("Processing metadata");
					long nameLength = readLength(fis);
					System.out.println("Metadata name length: " + nameLength);
					if (nameLength < 0 || nameLength > Integer.MAX_VALUE) {
						System.out.println("Invalid or EOF name length: " + nameLength);
						break;
					}
					fis.skip(nameLength);
					long valueLength = readLength(fis);
					System.out.println("Metadata value length: " + valueLength);
					if (valueLength >= 0 && valueLength <= Integer.MAX_VALUE) {
						fis.skip(valueLength);
					} else if (valueLength == -2) {
						System.out.println("Special encoding detected, no value skip");
					} else {
						System.out.println("Invalid or EOF value length: " + valueLength);
						break;
					}
				} else if (opcode == 0xFE) {
					System.out.println("Processing database section");
					long dbIndex = readLength(fis);
					System.out.println("Database index: " + dbIndex);
					int fb = fis.read();
					if (fb != 0xFB) throw new IOException("Expected FB, got 0x" + Integer.toHexString(fb));
					long hashTableSize = readLength(fis);
					long expireTableSize = readLength(fis);
					System.out.println("Hash table size: " + hashTableSize + ", Expire table size: " + expireTableSize);
					for (long i = 0; i < hashTableSize; i++) {
						int typeOrExpiry = fis.read();
						if (typeOrExpiry == -1) {
							System.out.println("EOF at typeOrExpiry, breaking");
							break;
						}
						System.out.println("Type or expiry: 0x" + Integer.toHexString(typeOrExpiry));
						long expiry = -1;
						if (typeOrExpiry == 0xFD) {
							expiry = readUnsignedInt(fis) * 1000L;
							typeOrExpiry = fis.read();
							if (typeOrExpiry == -1) {
								System.out.println("EOF after FD expiry, breaking");
								break;
							}
							System.out.println("FD expiry: " + expiry + ", next type: 0x" + Integer.toHexString(typeOrExpiry));
						} else if (typeOrExpiry == 0xFC) {
							expiry = readUnsignedLong(fis);
							typeOrExpiry = fis.read();
							if (typeOrExpiry == -1) {
								System.out.println("EOF after FC expiry, breaking");
								break;
							}
							System.out.println("FC expiry: " + expiry + ", next type: 0x" + Integer.toHexString(typeOrExpiry));
						}
						if (typeOrExpiry != 0x00) {
							throw new IOException("Unsupported value type: 0x" + Integer.toHexString(typeOrExpiry));
						}
						String key = readString(fis);
						String value = readString(fis);
						System.out.println("Read key: '" + key + "', value: '" + value + "', expiry: " + expiry);
						if (!key.isEmpty()) {
							map.put(key, new String[]{value, String.valueOf(expiry)});
							System.out.println("Stored key: " + key + " in map");
						} else {
							System.out.println("Skipping empty key");
						}
					}
				}
			}
			System.out.println("Map after loadRDB: " + map);
		} catch (IOException e) {
			System.out.println("Error reading rdb.RDB file: " + e.getMessage());
		}
	}

	private static String readString(FileInputStream fis) throws IOException {
		long length = readLength(fis);
		if (length == -1) {
			System.out.println("readString: EOF, returning empty string");
			return "";
		}
		if (length >= 0 && length <= Integer.MAX_VALUE) {
			byte[] bytes = new byte[(int) length];
			if (fis.read(bytes) != length) {
				System.out.println("readString: Incomplete read for length " + length);
				throw new IOException("Incomplete string read");
			}
			String result = new String(bytes, "UTF-8");
			System.out.println("readString: Read string '" + result + "' of length " + length);
			return result;
		}
		throw new IOException("Invalid string length: " + length);
	}

	// Read length-encoded value (big-endian)
	private static long readLength(FileInputStream fis) throws IOException {
		int firstByte = fis.read();
		if (firstByte == -1) {
			System.out.println("readLength: EOF");
			return -1;
		}
		int prefix = (firstByte >> 6) & 0x03;
		System.out.println("readLength: firstByte=0x" + Integer.toHexString(firstByte) + ", prefix=" + prefix);
		switch (prefix) {
			case 0:
				long len0 = firstByte & 0x3F;
				System.out.println("readLength: 6-bit length=" + len0);
				return len0;
			case 1:
				int nextByte = fis.read();
				if (nextByte == -1) {
					System.out.println("readLength: EOF in 14-bit length");
					return -1;
				}
				long len1 = ((firstByte & 0x3F) << 8) | nextByte;
				System.out.println("readLength: 14-bit length=" + len1);
				return len1;
			case 2:
				long len2 = readUnsignedInt(fis);
				System.out.println("readLength: 32-bit length=" + len2);
				return len2;
			case 3:
				int format = firstByte & 0x3F;
				if (format == 0x00) {
					int value = fis.read();
					if (value == -1) {
						System.out.println("readLength: EOF in 8-bit integer");
						return -1;
					}
					System.out.println("readLength: 8-bit integer=" + value);
					return -2; // Indicate special encoding
				}
				throw new IOException("Unsupported special string encoding: " + format);
			default:
				throw new IOException("Invalid length prefix: " + prefix);
		}
	}

	// Read 4-byte unsigned integer (big-endian)
	private static long readUnsignedInt(FileInputStream fis) throws IOException {
		byte[] bytes = new byte[4];
		if (fis.read(bytes) != 4) return -1;
		return ((bytes[0] & 0xFFL) << 24) | ((bytes[1] & 0xFFL) << 16) |
				((bytes[2] & 0xFFL) << 8) | (bytes[3] & 0xFFL);
	}

	// Read 8-byte unsigned long (little-endian for expiry)
	private static long readUnsignedLong(FileInputStream fis) throws IOException {
		byte[] bytes = new byte[8];
		if (fis.read(bytes) != 8) return -1;
		return ((bytes[7] & 0xFFL) << 56) | ((bytes[6] & 0xFFL) << 48) |
				((bytes[5] & 0xFFL) << 40) | ((bytes[4] & 0xFFL) << 32) |
				((bytes[3] & 0xFFL) << 24) | ((bytes[2] & 0xFFL) << 16) |
				((bytes[1] & 0xFFL) << 8) | (bytes[0] & 0xFFL);
	}
}
