package replication;

import execute.CommandExecutor;
import parser.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class MasterConnectionHandler implements Runnable {

	private final String masterHost;
	private final int masterPort;
	private final int replicaPort;
	private long replica_repl_offset;

	public MasterConnectionHandler(String masterHost, int masterPort, int replicaPort) {
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.replicaPort = replicaPort;
		this.replica_repl_offset = 0;
	}

	public void run() {
		try (Socket masterSocket = new Socket(masterHost, masterPort)) {
			InputStream in = masterSocket.getInputStream();
			OutputStream out = masterSocket.getOutputStream();
			String resp = "*1\r\n$4\r\nPING\r\n";
			out.write(resp.getBytes());
			out.flush();
			Parser.readLine(in);

			String listeningPort =
					"*3\r\n" +
							"$8\r\nREPLCONF\r\n" +
							"$14\r\nlistening-port\r\n" +
							"$" + String.valueOf(replicaPort).length() + "\r\n" + replicaPort + "\r\n";
			out.write(listeningPort.getBytes());
			out.flush();
			Parser.readLine(in);

			String capa =
					"*3\r\n" +
							"$8\r\nREPLCONF\r\n" +
							"$4\r\ncapa\r\n" +
							"$6\r\npsync2\r\n";
			out.write(capa.getBytes());
			out.flush();
			Parser.readLine(in);

			String psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
			out.write(psync.getBytes());
			out.flush();
			String fullresync = Parser.readLine(in);

			String lenLine = Parser.readLine(in);
			int len = Integer.parseInt(lenLine.substring(1));
			byte[] rdb = new byte[len];
			int read = 0;
			while (read < len) {
				int n = in.read(rdb, read, len - read);
				if (n <= 0) throw new IOException("Failed to read rdb.RDB");
				read += n;
			}

			while (true) {
				List<String> command = Parser.parseRESP(in);
				if (command == null || command.isEmpty()) continue;
				String cmd = command.get(0).toUpperCase();
				if (cmd.equals("REPLCONF") && command.size() > 1 && command.get(1).equalsIgnoreCase("GETACK")) {
					String ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n";
					String offset = String.valueOf(replica_repl_offset);
					ack += "$" + offset.length() + "\r\n" + offset + "\r\n";
					out.write(ack.getBytes());
					out.flush();
				} else {
					CommandExecutor.execute(command, null, null);
				}
				replica_repl_offset += Parser.getResp(command).getBytes().length;
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}
	}
}
