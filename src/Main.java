import server.Server;

public class Main {

	private static Server server;

	public static void main(String[] args) {
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		System.out.println("Logs from your program will appear here!");

		init(args);
		server.register();
		server.start();
	}

	private static void init (String[] args) {
		int port = 6379;
		String dir = null;
		String dbfilename = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--port") && i + 1 < args.length) {
				port = Integer.parseInt(args[++i]);
			} else if (args[i].equals("--replicaof") && i + 1 < args.length) {
				String[] masterInfo = args[++i].split(" ");
				String masterHost = masterInfo[0];
				int masterPort = Integer.parseInt(masterInfo[1]);
				server = new Server(masterHost, masterPort, port);
			} else if (args[i].equals("--dir") && i + 1 < args.length) {
				dir = args[++i];
			} else if (args[i].equals("--dbfilename") && i + 1 < args.length) {
				dbfilename = args[++i];
			}
		}

		if (server == null) {
			server = new Server(port, dir, dbfilename);
		}

	}
}
