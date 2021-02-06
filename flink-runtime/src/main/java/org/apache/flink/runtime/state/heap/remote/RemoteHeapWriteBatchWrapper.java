package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.util.Preconditions;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class RemoteHeapWriteBatchWrapper implements AutoCloseable {
	private static final int MIN_CAPACITY = 100;
	private static final int MAX_CAPACITY = 1000;
	private static final int PER_RECORD_BYTES = 100;
	// default 0 for disable memory size based flush
	private static final long DEFAULT_BATCH_SIZE = 0;

	private final Jedis db;

	private final Pipeline pipeline;

	private final int capacity;

	@Nonnegative
	private final long batchSize;

	private int numOperations;

	private int valueSize;

	public RemoteHeapWriteBatchWrapper(@Nonnull Jedis db, long batchSize) {
		this(db, 500, batchSize);
	}

	public RemoteHeapWriteBatchWrapper(@Nonnull Jedis db) {
		this(db, 500, DEFAULT_BATCH_SIZE);
	}

	public RemoteHeapWriteBatchWrapper(@Nonnull Jedis db, int capacity, long batchSize) {
		Preconditions.checkArgument(
			capacity >= MIN_CAPACITY && capacity <= MAX_CAPACITY,
			"capacity should be between " + MIN_CAPACITY + " and " + MAX_CAPACITY);
		Preconditions.checkArgument(batchSize >= 0, "Max batch size have to be no negative.");

		this.db = db;
		this.capacity = capacity;
		this.batchSize = batchSize;
		db.pipelined();
		this.pipeline = db.pipelined();
		this.numOperations = 0;
	}

	public void put(
		@Nonnull byte[] key,
		@Nonnull byte[] userKey,
		@Nonnull byte[] value) throws Exception {

		pipeline.hset(key, userKey, value);
		this.numOperations++;
		this.valueSize += value.length;
		flushIfNeeded();
	}

	public void remove(
		@Nonnull byte[] key,
		@Nonnull byte[] userKey
	) throws Exception {

		pipeline.hdel(key, userKey);
		this.numOperations++;
		flushIfNeeded();
	}


	@Override
	public void close() {
		if (numOperations != 0) {
			this.pipeline.sync();
			this.numOperations = 0;
			this.valueSize = 0;
		}
		pipeline.close();
	}

	public void flush() throws Exception {
		this.pipeline.sync();
		this.numOperations = 0;
		this.valueSize = 0;
	}

	private void flushIfNeeded() {
		boolean needFlush =
			this.numOperations == capacity || (batchSize > 0 && valueSize >= batchSize);
		if (needFlush) {
			this.pipeline.sync();
		}
		this.numOperations = 0;
		this.valueSize = 0;
	}

}
