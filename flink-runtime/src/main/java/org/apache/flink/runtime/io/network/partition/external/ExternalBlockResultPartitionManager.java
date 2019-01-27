/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExternalBlockResultPartitionManager implements ResultPartitionProvider {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockResultPartitionManager.class);

	private final ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration;

	private final LocalResultPartitionResolver resultPartitionResolver;

	/** Each directory has its group of threads to do disk IO operations. */
	@VisibleForTesting
	final Map<String, ThreadPoolExecutor> dirToThreadPool = new HashMap<>();

	/** Cache file meta for result partitions. */
	@VisibleForTesting
	final ConcurrentHashMap<ResultPartitionID, ExternalBlockResultPartitionMeta>
		resultPartitionMetaMap = new ConcurrentHashMap<>();

	/** The buffer pool to read data into. */
	@VisibleForTesting
	final FixedLengthBufferPool bufferPool;

	/** Periodically recycle result partitions. */
	private final ScheduledExecutorService resultPartitionRecyclerExecutorService;

	private final AtomicBoolean isStopped = new AtomicBoolean(false);

	public ExternalBlockResultPartitionManager(
		ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration) throws Exception {

		this.shuffleServiceConfiguration = shuffleServiceConfiguration;
		this.resultPartitionResolver = LocalResultPartitionResolverFactory.create(shuffleServiceConfiguration);

		// Init the buffer pool
		this.bufferPool = new FixedLengthBufferPool(
			shuffleServiceConfiguration.getBufferNumber(),
			shuffleServiceConfiguration.getMemorySizePerBufferInBytes(),
			MemoryType.OFF_HEAP);

		constructThreadPools();

		this.resultPartitionRecyclerExecutorService = Executors.newSingleThreadScheduledExecutor();
		this.resultPartitionRecyclerExecutorService.scheduleWithFixedDelay(
			() -> recycleResultPartitions(),
			0,
			shuffleServiceConfiguration.getDiskScanIntervalInMS(),
			TimeUnit.MILLISECONDS);

		LOG.info("Final configurations: " + shuffleServiceConfiguration);
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
		ResultPartitionID resultPartitionId,
		int index,
		BufferAvailabilityListener availabilityListener) throws IOException {

		// Reject all the requests if shuffle service is stopping.
		if (isStopped.get()) {
			throw new IOException("ExternalBlockResultPartitionManager has already been stopped.");
		}

		ExternalBlockResultPartitionMeta resultPartitionMeta = resultPartitionMetaMap.get(resultPartitionId);
		if (resultPartitionMeta == null) {
			LocalResultPartitionResolver.ResultPartitionFileInfo fileInfo = resultPartitionResolver.getResultPartitionDir(
				resultPartitionId);
			resultPartitionMeta = new ExternalBlockResultPartitionMeta(
				resultPartitionId,
				shuffleServiceConfiguration.getFileSystem(),
				fileInfo);
			ExternalBlockResultPartitionMeta prevResultPartitionMeta =
				resultPartitionMetaMap.putIfAbsent(resultPartitionId, resultPartitionMeta);
			if (prevResultPartitionMeta != null) {
				resultPartitionMeta = prevResultPartitionMeta;
			}
		}

		ExternalBlockSubpartitionView subpartitionView = new ExternalBlockSubpartitionView(
			resultPartitionMeta,
			index,
			dirToThreadPool.get(resultPartitionMeta.getRootDir()),
			resultPartitionId,
			bufferPool,
			shuffleServiceConfiguration.getWaitCreditDelay(),
			availabilityListener);

		resultPartitionMeta.notifySubpartitionStartConsuming(index);

		return subpartitionView;
	}

	/**
	 * This method is used to create a mapping between yarnAppId and user.
	 */
	public void initializeApplication(String user, String appId) {
		resultPartitionResolver.initializeApplication(user, appId);
	}

	/**
	 * This method is used to remove both in-memory meta info and local files when
	 * this application is stopped in Yarn.
	 */
	public void stopApplication(String appId) {
		Set<ResultPartitionID> resultPartitionIDS = resultPartitionResolver.stopApplication(appId);
		if (!resultPartitionIDS.isEmpty()) {
			resultPartitionIDS.forEach(resultPartitionID -> {
				resultPartitionMetaMap.remove(resultPartitionID);
			});
		}
	}

	public void stop() {
		LOG.warn("Stop ExternalBlockResultPartitionManager, probably ShuffleService is stopped");
		try {
			boolean succ = isStopped.compareAndSet(false, true);
			if (!succ) {
				LOG.info("ExternalBlockResultPartitionManager has already been stopped.");
				return;
			}

			// Stop disk IO threads immediately
			Iterator threadPoolIter = dirToThreadPool.entrySet().iterator();
			while (threadPoolIter.hasNext()) {
				Map.Entry entry = (Map.Entry)threadPoolIter.next();
				((ThreadPoolExecutor) entry.getValue()).shutdownNow();
			}

			resultPartitionRecyclerExecutorService.shutdownNow();

			resultPartitionResolver.stop();

			bufferPool.lazyDestroy();

			resultPartitionMetaMap.clear();
		} catch (Throwable e) {
			LOG.error("Exception occurs when stopping ExternalBlockResultPartitionManager", e);
		}
	}

	// ------------------------------------ Internal Utilities ------------------------------------

	/**
	 * This method is called only in constructor to construct thread pools for disk IO threads.
	 */
	private void constructThreadPools() {
		ThreadGroup threadGroup = new ThreadGroup("Disk IO Thread Group");
		shuffleServiceConfiguration.getDirToDiskType().forEach((dir, diskType) -> {
			Integer threadNum = shuffleServiceConfiguration.getDiskTypeToIOThreadNum().get(diskType);
			Comparator subpartitionViewComparator = shuffleServiceConfiguration.newSubpartitionViewComparator();
			BlockingQueue<Runnable> blockingQueue = subpartitionViewComparator != null ?
				new PriorityBlockingQueue<>(200, subpartitionViewComparator) :
				new LinkedBlockingQueue<>();
			ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
				threadNum, threadNum, 0L, TimeUnit.MILLISECONDS, blockingQueue,
				new DispatcherThreadFactory(threadGroup, "IO thread [" + diskType + "] [" + dir + "]"));
			dirToThreadPool.put(dir, threadPool);
		});
	}

	@VisibleForTesting
	void recycleResultPartitions() {
		long currTime = System.currentTimeMillis();
		HashMap<ResultPartitionID, ExternalBlockResultPartitionMeta> consumedPartitionsToRemove = new HashMap<>();
		HashMap<ResultPartitionID, ExternalBlockResultPartitionMeta> partialConsumedPartitionsToRemove = new HashMap<>();

		for (Map.Entry<ResultPartitionID, ExternalBlockResultPartitionMeta> partitionEntry : resultPartitionMetaMap.entrySet()) {
			ResultPartitionID resultPartitionID = partitionEntry.getKey();
			ExternalBlockResultPartitionMeta resultPartitionMeta = partitionEntry.getValue();
			if (!resultPartitionMeta.hasInitialized()) {
				continue;
			}
			int refCnt = resultPartitionMeta.getReferenceCount();
			if (refCnt > 0) {
				// Skip because some subpartition views are consuming subpartitions.
				continue;
			}
			int unconsumedSubpartitionCount = resultPartitionMeta.getUnconsumedSubpartitionCount();
			if (unconsumedSubpartitionCount <= 0) {
				// It seems all the down streams have fetched their data from this result partition.
				long lastActiveTimeInMs = resultPartitionMeta.getLastActiveTimeInMs();
				// we may get -1L in a rare condition due to decreasing count and setting timestamp without lock
				if ((currTime -lastActiveTimeInMs) > resultPartitionMeta.getConsumedPartitionTTL()) {
					consumedPartitionsToRemove.put(resultPartitionID, resultPartitionMeta);
				}
			} else {
				// There are subpartitions left to be consumed. If this job fails, such partition
				// will never be fully consumed.
				long lastActiveTimeInMs = resultPartitionMeta.getLastActiveTimeInMs();
				if ((currTime - lastActiveTimeInMs) > resultPartitionMeta.getPartialConsumedPartitionTTL()) {
					partialConsumedPartitionsToRemove.put(resultPartitionID, resultPartitionMeta);
				}
			}
		}

		removeResultPartitionAndMeta(consumedPartitionsToRemove,
			"CONSUMED_PARTITION_TTL_TIMEOUT",
			LOG.isDebugEnabled());
		removeResultPartitionAndMeta(partialConsumedPartitionsToRemove,
			"PARTIAL_CONSUMED_PARTITION_TTL_TIMEOUT",
			true);
	}

	private void removeResultPartitionAndMeta(
		HashMap<ResultPartitionID, ExternalBlockResultPartitionMeta> partitionsToRemove,
		String recycleReason,
		boolean printLog) {

		if (partitionsToRemove.isEmpty()) {
			return;
		}

		Iterator<Map.Entry<ResultPartitionID, ExternalBlockResultPartitionMeta>> iterator =
			partitionsToRemove.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<ResultPartitionID, ExternalBlockResultPartitionMeta> entry = iterator.next();
			ResultPartitionID resultPartitionID = entry.getKey();
			ExternalBlockResultPartitionMeta meta = entry.getValue();
			// double check reference count
			if (meta.getReferenceCount() > 0) {
				iterator.remove();
			} else {
				resultPartitionMetaMap.remove(resultPartitionID);
				resultPartitionResolver.recycleResultPartition(resultPartitionID);
				if (printLog) {
					LOG.info("Delete partition's directory: {}, reason: {}, lastActiveTime: {}",
						meta.getResultPartitionDir(), recycleReason, meta.getLastActiveTimeInMs());
				}
			}
		}
	}
}
