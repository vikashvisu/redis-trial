package config;

import subscription.Subscription;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;

public class Config {

	protected int port;
	private long master_repl_offset = 0;
	private String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
	private String dir;
	private String dbfilename;

	private String masterHost;
	private int masterPort;
	private boolean isReplica = false;

	private final HashMap<String, String> redisKeys = new HashMap<>();

	public HashMap<String, String> getRedisKeys() {
		return redisKeys;
	}

	public long getMaster_repl_offset() {
		return master_repl_offset;
	}

	public String getMaster_replid() {
		return master_replid;
	}

	public String getMasterHost() {
		return masterHost;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public String getDir() {
		return dir;
	}

	public String getDbfilename() {
		return dbfilename;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setDir(String dir) {
		this.dir = dir;
	}

	public void setDbfilename(String dbfilename) {
		this.dbfilename = dbfilename;
	}

	public void setMasterHost(String masterHost) {
		this.masterHost = masterHost;
	}

	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}

	public void setReplica(boolean replica) {
		isReplica = replica;
	}

	public boolean isReplica() {
		return isReplica;
	}

	public void setMaster_repl_offset(long master_repl_offset) {
		this.master_repl_offset = master_repl_offset;
	}

	public String ping (List<String> command, SocketChannel s, SelectionKey selectKey) {
		if (s != null && Subscription.isSubMode(selectKey)) {
			String payload = command.size() > 1 ? command.get(1) : null;
			String resp = "*2" + "\r\n" + "$4\r\n" + "pong" + "\r\n";
			resp += payload == null ? "$0\r\n\r\n" : "$" + payload.length() + "\r\n" + payload + "\r\n";
			return resp;
		}
		return "+PONG\r\n";
	}

	public String echo (List<String> command) {
		String arg = command.get(1);
		return "$" + arg.length() + "\r\n" + arg + "\r\n";
	}

	public String type (List<String> command) {
		String key = command.get(1);
		if (redisKeys.containsKey(key)) return "+" + redisKeys.get(key) + "\r\n";
		return "+none\r\n";
	}

	public String info (List<String> command) {
		if (command.get(1).equalsIgnoreCase("replication")) {
			String role = isReplica ? "slave" : "master";
			String infoRepl = "+role:" + role + "\r\n" + "+master_replid:" + master_replid + "\r\n" + "+master_repl_offset:" + master_repl_offset + "\r\n";
			return "$" + infoRepl.length() + "\r\n" + infoRepl + "\r\n";
		}
		return null;
	}

	public String config (List<String> command) {
		if (command.size() == 3 && command.get(1).equalsIgnoreCase("get") && command.get(2).equalsIgnoreCase("dir")) {
			String dirValue = dir != null ? dir : "";
			return "*2\r\n$3\r\ndir\r\n$" + dirValue.length() + "\r\n" + dirValue + "\r\n";
		}
		return null;
	}

}
