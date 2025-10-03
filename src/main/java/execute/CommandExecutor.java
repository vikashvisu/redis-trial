package execute;

import constant.CHECK;
import constant.OPERATION;
import constant.SUBSCRIPTION;
import list.RedisList;
import multi.MultiExecutor;
import server.Server;
import store.Store;
import stream.StreamStore;
import subscription.Subscription;
import zset.GeoQuery;
import zset.Zset;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;

public class CommandExecutor {

	public static String execute (List<String> command, SocketChannel s, SelectionKey selectKey) {
		try {
			String cmd = command.getFirst().toUpperCase();


			return switch (cmd) {

				case CHECK.PING -> Server.ping(command, s, selectKey);
				case CHECK.ECHO -> Server.echo(command);
				case CHECK.TYPE -> Server.type(command);
				case CHECK.INFO -> Server.info(command);
				case CHECK.CONFIG -> Server.config(command);
				case SUBSCRIPTION.PUBLISH -> Subscription.publish(command);
				case SUBSCRIPTION.SUBSCRIBE -> Subscription.subscribe(command, selectKey);
				case SUBSCRIPTION.UNSUBSCRIBE -> Subscription.unsubscribe(command, selectKey);
				case OPERATION.MULTI -> MultiExecutor.multi(selectKey);
				case OPERATION.SET -> Store.set(command);
				case OPERATION.GET -> Store.get(command);
				case OPERATION.INCR -> Store.incr(command);
				case OPERATION.KEYS -> Store.keys(command);
				case OPERATION.ZADD -> Zset.zadd(command);
				case OPERATION.ZRANK -> Zset.zrank(command);
				case OPERATION.ZREM -> Zset.zrem(command);
				case OPERATION.ZCARD -> Zset.zcard(command);
				case OPERATION.ZSCORE -> Zset.zscore(command);
				case OPERATION.ZRANGE -> Zset.zrange(command);
				case OPERATION.GEOADD -> GeoQuery.geoadd(command);
				case OPERATION.GEOPOS -> GeoQuery.geopos(command);
				case OPERATION.GEODIST -> GeoQuery.geodist(command);
				case OPERATION.GEOSEARCH -> GeoQuery.geosearch(command);
				case OPERATION.RPUSH -> RedisList.rpush(command);
				case OPERATION.LPUSH -> RedisList.lpush(command);
				case OPERATION.LPOP -> RedisList.lpop(command);
				case OPERATION.BLPOP -> RedisList.blpop(command, selectKey);
				case OPERATION.LLEN -> RedisList.llen(command);
				case OPERATION.LRANGE -> RedisList.lrange(command);
				case OPERATION.XADD -> StreamStore.xadd(command);
				case OPERATION.XRANGE -> StreamStore.xrange(command);
				case OPERATION.XREAD -> StreamStore.xread(command, selectKey);

				default -> null;
			};
		} catch (Exception e) {
			System.out.println("IOException: " + e.getMessage());
			return null;
		}
	}


}
