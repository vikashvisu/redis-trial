package clients;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class ClientManager {

	private static final Set<SocketChannel> clients = new HashSet<>();

	public static Set<SocketChannel> getClients() {
		return clients;
	}

	public static void writeResponseToClient (String resp, SelectionKey key) {
		SocketChannel client = (SocketChannel) key.channel();
		try {
			ByteBuffer response = ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8));
			while (response.hasRemaining()) {
				client.write(response);
			}
		} catch (IOException e) {
			try {
				clients.remove(client);
				client.close();
				key.cancel();
			} catch (IOException ignored) {}
			System.out.println("IOException: " + e.getMessage());
		}

	}
}
