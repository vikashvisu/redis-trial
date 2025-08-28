import java.net.Socket;

class PopTicket {
	private final Socket socket;
	private final double expTime;

	PopTicket (Socket socket, double expTime) {
		this.socket = socket;
		this.expTime = expTime;
	}

	public double getExpTime() {
		return expTime;
	}

	public Socket getSocket() {
		return socket;
	}
}

