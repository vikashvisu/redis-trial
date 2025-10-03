package multi;

import clients.ClientManager;
import execute.CommandExecutor;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;

public class MultiExecutor {

	private static final HashMap<SelectionKey, MultiHandler> multiHandlers = new HashMap<>();

	public static String multi (SelectionKey key) {
		System.out.println("Selection key: " + key);
		multiHandlers.putIfAbsent(key, new MultiHandler());
		multiHandlers.get(key).init();
		return "+OK\r\n";
	}

	public static void discard (SelectionKey key) {
		String resp = "";
		if (!multiHandlers.containsKey(key) || !multiHandlers.get(key).isOn()) {
			resp = "-ERR DISCARD without MULTI\r\n";
		} else {
			multiHandlers.get(key).clear();
			resp = "+OK\r\n";
		}
		ClientManager.writeResponseToClient(resp, key);
	}

	public static void exec (List<String> command, SelectionKey key, SocketChannel client) {

		try {
			if (!multiHandlers.containsKey(key) || !multiHandlers.get(key).isOn()) {
				ClientManager.writeResponseToClient("-ERR EXEC without MULTI\r\n", key);
			} else if (multiHandlers.get(key).isEmpty()) {
				multiHandlers.get(key).clear();
				ClientManager.writeResponseToClient("*0\r\n", key);
			} else {
				String resp = "*" + multiHandlers.get(key).getSize() + "\r\n";
				while (!multiHandlers.get(key).isEmpty()) {
					List<String> args = multiHandlers.get(key).getNext();
					String res = CommandExecutor.execute(args, client, key);
					if (res == null) {
						throw new IOException("unknown command: " + command);
					} else {
						resp += res;
					}
				}
				ClientManager.writeResponseToClient(resp, key);
				multiHandlers.get(key).clear();
			}
		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		}

	}

	public static boolean isMultiModeOn (SelectionKey key) {
		return multiHandlers.containsKey(key) && multiHandlers.get(key).isOn();
	}

	public static String addToQueue (List<String> command, SelectionKey key) {
		multiHandlers.get(key).add(command);
		return "+QUEUED\r\n";
	}


}
