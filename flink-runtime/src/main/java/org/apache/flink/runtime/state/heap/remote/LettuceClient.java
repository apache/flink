package org.apache.flink.runtime.state.heap.remote;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LettuceClient implements RemoteKVSyncClient, RemoteKVAsyncClient {

	private static final Logger LOG = LoggerFactory.getLogger(LettuceClient.class);

	private RedisClient db;

	private StatefulRedisConnection<byte[], byte[]> connection;

	private RedisAsyncCommands<byte[], byte[]> commands;

	private ArrayList<RedisFuture<?>> cachedFutures = new ArrayList<>();

	@Override
	public byte[] get(byte[] key) {
		try {
			return commands.get(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object set(byte[] key, byte[] value) {
		try {
			return commands.set(key, value).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		try {
			return commands.hget(key, field).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		try {
			return commands.hgetall(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object hset(byte[] key, byte[] field, byte[] value) {
		try {
			return commands.hset(key, field, value).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<byte[]> hkeys(byte[] key) {
		try {
			return commands.hkeys(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object hdel(byte[] key, byte[]... fields) {
		try {
			return commands.hdel(key, fields).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		try {
			return commands.hexists(key, field).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object del(byte[] key) {
		try {
			return commands.del(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long dbSize() {
		try {
			return commands.dbsize().toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<String> keys(String predicate) {
		try {
			return commands.keys(predicate.getBytes()).toCompletableFuture().get().stream().map(x->x.toString()).collect(
				Collectors.toSet());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long rpush(byte[] key, byte[]... strings) {
		try {
			return commands.rpush(key, strings).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long lpush(byte[] key, byte[]... strings) {
		try {
			return commands.lpush(key, strings).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void pipelineHSet(byte[] key, byte[] field, byte[] value) {
		commands.setAutoFlushCommands(false);
		cachedFutures.add(commands.hset(key, field, value));
	}

	@Override
	public void pipelineHDel(byte[] key, byte[] field) {
		commands.setAutoFlushCommands(false);
		cachedFutures.add(commands.hdel(key, field));
	}

	@Override
	public void pipelineSync() {
		commands.flushCommands();
		LettuceFutures.awaitAll(100000, TimeUnit.SECONDS,
			cachedFutures.toArray(new RedisFuture[cachedFutures.size()]));
		commands.setAutoFlushCommands(true);
	}

	@Override
	public void pipelineClose() { }

	@Override
	public void openDB(String host) {
		RedisURI redisUri = RedisURI.Builder.redis("127.0.0.1", 6379).withPassword("authentication").build();
		db = RedisClient.create(redisUri);
		connection = db.connect(new ByteArrayCodec());
		commands = connection.async();
		LOG.info("Connection from Lettuce Client to Redis Cluster {} successful.", host);
	}

	@Override
	public void closeDB() {
		connection.close();
	}

	@Override
	public CompletableFuture<byte[]> getAsync(byte[] key) {
		return commands.get(key).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<String> setAsync(byte[] key, byte[] value) {
		return commands.set(key, value).toCompletableFuture();
	}

	@Override
	public CompletableFuture<byte[]> hgetAsync(byte[] key, byte[] field) {
		return commands.hget(key, field).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Map<byte[], byte[]>> hgetAllAsync(byte[] key) {
		return commands.hgetall(key).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<Boolean> hsetAsync(byte[] key, byte[] field, byte[] value) {
		return commands.hset(key, field, value).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Collection<byte[]>> hkeysAsync(byte[] key) {;
		return commands.hkeys(key).toCompletableFuture().thenApply(x->x);
	}

	@Nullable
	@Override
	public CompletableFuture<Long> hdelAsync(byte[] key, byte[]... fields) {
		return commands.hdel(key, fields).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Boolean> hexistsAsync(byte[] key, byte[] field) {
		return commands.hexists(key, field).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<Long> delAsync(byte[] key) {
		return commands.del(key).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Long> rpushAsync(byte[] key, byte[]... strings) {
		return commands.rpush(key, strings).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Long> lpushAsync(byte[] key, byte[]... strings) {
		return commands.lpush(key, strings).toCompletableFuture();
	}
}
