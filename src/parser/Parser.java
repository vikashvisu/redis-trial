package parser;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Parser {

	public static List<String> parseRESP(InputStream in) throws IOException {
		String firstLine = readLine(in);
		if (firstLine == null) return null;

		if (firstLine.startsWith("*")) {
			int argCount = Integer.parseInt(firstLine.substring(1));
			List<String> args = new ArrayList<>();
			for (int i = 0; i < argCount; i++) {
				String lenLine = readLine(in);
				if (!lenLine.startsWith("$")) {
					throw new IOException("Invalid bulk string length line: " + lenLine);
				}
				int length = Integer.parseInt(lenLine.substring(1));
				if (length == -1) {
					args.add(null);
					readLine(in); // \r\n
					continue;
				}
				byte[] buf = new byte[length];
				int r = 0;
				while (r < length) {
					int n = in.read(buf, r, length - r);
					if (n <= 0) throw new IOException("Failed to read bulk string");
					r += n;
				}
				args.add(new String(buf, "UTF-8"));
				String crlf = readLine(in);
				if (!crlf.isEmpty()) {
					throw new IOException("Expected \r\n after bulk string");
				}
			}
			return args;
		} else {
			throw new IOException("Invalid RESP input: " + firstLine);
		}
	}

	public static String readLine(InputStream in) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while (true) {
			int b = in.read();
			if (b == -1) {
				if (baos.size() == 0) return null;
				throw new EOFException("Unexpected EOF while reading line");
			}
			if (b == '\r') {
				int next = in.read();
				if (next == -1) {
					throw new EOFException();
				}
				if (next == '\n') {
					break;
				}
				baos.write(b);
				baos.write(next);
				continue;
			}
			baos.write(b);
		}
		return new String(baos.toByteArray(), "UTF-8");
	}

	public static List<String> parseRESP(StringBuilder sb) {
		if (sb.isEmpty())
			return null;

		if (sb.charAt(0) == '*') {
			int lineEnd = sb.indexOf("\r\n");
			if (lineEnd == -1)
				return null;

			int numElements;
			try {
				numElements = Integer.parseInt(sb.substring(1, lineEnd));
			} catch (NumberFormatException e) {
				return null; // malformed header
			}

			List<String> parts = new ArrayList<>();
			int pos = lineEnd + 2;

			for (int i = 0; i < numElements; i++) {
				if (pos >= sb.length() || sb.charAt(pos) != '$')
					return null;

				int lenEnd = sb.indexOf("\r\n", pos);
				if (lenEnd == -1)
					return null;

				int bulkLen;
				try {
					bulkLen = Integer.parseInt(sb.substring(pos + 1, lenEnd));
				} catch (NumberFormatException e) {
					return null;
				}
				pos = lenEnd + 2;

				if (bulkLen < 0) {
					// NULL bulk string ($-1), represent as null
					parts.add(null);
					continue;
				}

				if (pos + bulkLen + 2 > sb.length())
					return null; // not all bytes arrived yet

				String bulkStr = sb.substring(pos, pos + bulkLen);
				parts.add(bulkStr);

				pos += bulkLen + 2; // skip bulk data and trailing \r\n
			}

			sb.delete(0, pos); // remove parsed command
			return parts;
		} else {
			// Inline command (like "PING\r\n")
			int lineEnd = sb.indexOf("\r\n");
			if (lineEnd == -1)
				return null;
			String line = sb.substring(0, lineEnd).trim();
			sb.delete(0, lineEnd + 2);
			if (line.isEmpty()) return Collections.emptyList();
			return Arrays.asList(line.split("\\s+"));
		}
	}

	public static String getResp(List<String> command) {
		String resp = "*" + command.size() + "\r\n";
		for (String comp : command) {
			resp += "$" + comp.length() + "\r\n" + comp + "\r\n";
		}
		return resp;
	}
}
