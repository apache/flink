package org.apache.flink.runtime.state.heap.remote;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

public interface RemoteKVSyncClient extends RemoteKVClient {

	byte[] get(byte[] key);

	@Nullable
	Object set(byte[] key, byte[] value);

	byte[] hget(byte[] key, byte[] field);

	Map<byte[], byte[]> hgetAll(byte[] key);

	@Nullable
	Object hset(byte[] key, byte[] field, byte[] value);

	Collection<byte[]> hkeys(byte[] key);

	@Nullable
	Object hdel(byte[] key, byte[]... fields);

	Boolean hexists(byte[] key, byte[] field);

	@Nullable
	Object del(byte[] key);

	Long rpush(byte[] key, byte[]... strings);

	Long lpush(byte[] key, byte[]... strings);

	void pipelineHSet(byte[] key, byte[] field, byte[] value);

	void pipelineHDel(byte[] key, byte[] field);
}
