package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Map;

public class RedisOperationUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RedisOperationUtils.class);

	public static Jedis openDB(String host) throws IOException {
		Jedis db = null;
		try {
			db = new Jedis(host);
			LOG.info("Connection in Redis Server {} successful.", host);
		} catch (Exception e) {
			throw new IOException("Failed to connect: Bye");
		}
		return db;
	}

	/**
	 * Creates a state info from a new meta info to use with a k/v state.
	 *
	 * <p>Creates the column family for the state.
	 * Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
	 */
	public static RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo createStateInfo(
		RegisteredStateMetaInfoBase metaInfoBase) {
		byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
		return new RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo(metaInfoBase, nameBytes);
	}

	public static void registerKvStateInformation(
		Map<String, RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo> kvStateInformation,
		String stateDesc,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo stateinfo) {
		kvStateInformation.put(stateDesc, stateinfo);
	}
}
