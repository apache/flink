package org.apache.flink.runtime.state.heap.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class JedisClusterSyncClient implements RemoteKVSyncClient {
	private static final Logger LOG = LoggerFactory.getLogger(JedisClusterSyncClient.class);

	private JedisCluster db;

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
		AtomicLong total = new AtomicLong();
		db.getClusterNodes().entrySet().forEach(x->{
			total.addAndGet(x.getValue().getResource().dbSize());
		});
		return total.get();
	}

	@Override
	public Collection<String> keys(String predicate) {
		Set<String> ret = new HashSet<String>();
		db.getClusterNodes().values().stream().forEach(node->{
			Set<String> localSet = node.getResource().keys(predicate);
			synchronized (ret){
				ret.addAll(localSet);
			}
		});
		return ret;
	}

	@Override
	public Long rpush(byte[] key, byte[]... strings) { return db.rpush(key, strings); }

	@Override
	public Long lpush(byte[] key, byte[]... strings) { return db.lpush(key, strings); }

	@Override
	public void pipelineHSet(byte[] key, byte[] field, byte[] value){
		try {
			throw new Exception("Pipeline Operator Not Supported For Jedis Cluster");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pipelineHDel(byte[] key, byte[] field) {
		try {
			throw new Exception("Pipeline Operator Not Supported For Jedis Cluster");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pipelineSync() {
		try {
			throw new Exception("Pipeline Operator Not Supported For Jedis Cluster");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pipelineClose() {
		try {
			throw new Exception("Pipeline Operator Not Supported For Jedis Cluster");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void openDB(String host) throws IOException {
		try {
			db = new JedisCluster(new HostAndPort(host, 12345));
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
