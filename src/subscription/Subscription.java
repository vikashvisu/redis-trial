package subscription;

import clients.ClientManager;

import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Subscription {

	private static final HashMap<String, Set<SelectionKey>> channelToSubs = new HashMap<>();
	private static final HashMap<SelectionKey, Set<String>> subToChannels = new HashMap<>();

	public static String subscribe(List<String> command, SelectionKey key) {
		String channel = command.get(1);
		channelToSubs.putIfAbsent(channel, new HashSet<>());
		channelToSubs.get(channel).add(key);

		subToChannels.putIfAbsent(key, new HashSet<>());
		subToChannels.get(key).add(channel);

		int channelCount = subToChannels.get(key).size();
		String resp = "*3"+ "\r\n" + "$9" + "\r\n" + "subscribe" + "\r\n";
		resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
		resp += ":" + channelCount + "\r\n";
		return resp;
	}

	public static String unsubscribe(List<String> command, SelectionKey key) {
		String channel = command.get(1);

		if (channelToSubs.containsKey(channel)) {
			channelToSubs.get(channel).remove(key);
			if  (channelToSubs.get(channel).isEmpty()) {
				channelToSubs.remove(channel);
			}
		}


		if (subToChannels.containsKey(key)) {
			subToChannels.get(key).remove(channel);
			if  (subToChannels.get(key).isEmpty()) {
				subToChannels.remove(key);
			}
		}
		int channelCount = subToChannels.containsKey(key) ? subToChannels.get(key).size() : 0;
		String resp = "*3"+ "\r\n" + "$11" + "\r\n" + "unsubscribe" + "\r\n";
		resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
		resp += ":" + channelCount + "\r\n";
		return resp;
	}

	public static boolean isSubMode(SelectionKey skey) {
		return subToChannels.containsKey(skey);
	}

	public static String publish (List<String> command) {
		String channel = command.get(1);
		String msg = command.get(2);
		int subCount = 0;

		Set<SelectionKey> channels = channelToSubs.get(channel);
		String resp = "*3\r\n$7\r\nmessage\r\n";
		resp += "$" + channel.length() + "\r\n" + channel + "\r\n";
		resp += "$" + msg.length() + "\r\n" + msg + "\r\n";
		for  (SelectionKey skey : channels) {
			ClientManager.writeResponseToClient(resp, skey);
		}
		subCount = channelToSubs.get(channel).size();

		return ":" + subCount + "\r\n";
	}


}
