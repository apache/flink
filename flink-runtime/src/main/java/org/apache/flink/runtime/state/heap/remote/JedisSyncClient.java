package org.apache.flink.runtime.state.heap.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class JedisSyncClient implements RemoteKVSyncClient {
	private static final Logger LOG = LoggerFactory.getLogger(JedisClusterSyncClient.class);

	private Jedis db;

	private Pipeline pipeline;

	@Override
	public byte[] get(byte[] key) {
		return db.get(key);
	}

	@Nullable
	@Override
	public Object set(byte[] key, byte[] value) { return db.set(key, value); }

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		return db.hget(key, field);
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		return db.hgetAll(key);
	}

	@Nullable
	@Override
	public Object hset(byte[] key, byte[] field, byte[] value) {
		return db.hset(key, field, value);
	}

	@Override
	public Collection<byte[]> hkeys(byte[] key) {
		return db.hkeys(key);
	}

	@Nullable
	@Override
	public Object hdel(byte[] key, byte[]... fields) { return db.hdel(key, fields); }

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		return db.hexists(key, field);
	}

	@Nullable
	@Override
	public Object del(byte[] key) {
		return db.del(key);
	}

	@Override
	public Long dbSize() {
		return db.dbSize();
	}

	@Override
	public Collection<String> keys(String predicate) {
		return db.keys(predicate);
	}

	@Override
	public Long rpush(byte[] key, byte[]... strings) { return db.rpush(key, strings); }

	@Override
	public Long lpush(byte[] key, byte[]... strings) { return db.lpush(key, strings); }

	@Override
	public void pipelineHSet(byte[] key, byte[] field, byte[] value){
		pipeline.hset(key, field, value);
	}

	@Override
	public void pipelineHDel(byte[] key, byte[] field) {
		pipeline.hdel(key, field);
	}

	@Override
	public void pipelineSync() {
		pipeline.sync();
	}

	@Override
	public void pipelineClose() {
		pipeline.close();
	}

	@Override
	public void openDB(String host) throws IOException {
		try {
			db = new Jedis(host);
			pipeline = db.pipelined();
			LOG.info("Connection in Redis Server {} successful.", host);
		} catch (Exception e) {
			throw new IOException("Failed to connect: Bye");
		}
	}

	@Override
	public void closeDB() {
		db.close();
	}

}
