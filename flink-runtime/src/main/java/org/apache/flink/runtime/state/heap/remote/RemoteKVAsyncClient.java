package org.apache.flink.runtime.state.heap.remote;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface RemoteKVAsyncClient extends RemoteKVClient {

	CompletableFuture<byte[]> getAsync(byte[] key);

	@Nullable
	CompletableFuture<String> setAsync(byte[] key, byte[] value);

	CompletableFuture<byte[]> hgetAsync(byte[] key, byte[] field);

	CompletableFuture<Map<byte[], byte[]>> hgetAllAsync(byte[] key);

	@Nullable
	CompletableFuture<Boolean> hsetAsync(byte[] key, byte[] field, byte[] value);

	CompletableFuture<Collection<byte[]>> hkeysAsync(byte[] key);

	@Nullable
	CompletableFuture<Long> hdelAsync(byte[] key, byte[]... fields);

	CompletableFuture<Boolean> hexistsAsync(byte[] key, byte[] field);

	@Nullable
	CompletableFuture<Long> delAsync(byte[] key);

	CompletableFuture<Long> rpushAsync(byte[] key, byte[]... strings);

	CompletableFuture<Long> lpushAsync(byte[] key, byte[]... strings);
}
