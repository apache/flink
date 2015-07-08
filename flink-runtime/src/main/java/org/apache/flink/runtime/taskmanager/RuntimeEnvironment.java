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

package org.apache.flink.runtime.taskmanager;

import akka.actor.ActorRef;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.SmallAccumulatorEvent;
import org.apache.flink.runtime.accumulators.LargeAccumulatorEvent;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.messages.accumulators.ReportSmallAccumulatorResult;
import org.apache.flink.runtime.messages.accumulators.ReportLargeAccumulatorResult;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.SerializedValue;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * In implementation of the {@link Environment}.
 */
public class RuntimeEnvironment implements Environment {
	
	private final JobID jobId;
	private final JobVertexID jobVertexId;
	private final ExecutionAttemptID executionId;
	
	private final String taskName;
	private final String taskNameWithSubtasks;
	private final int subtaskIndex;
	private final int parallelism;
	
	private final Configuration jobConfiguration;
	private final Configuration taskConfiguration;
	
	private final ClassLoader userCodeClassLoader;

	private final MemoryManager memManager;
	private final IOManager ioManager;
	private final BroadcastVariableManager bcVarManager;
	private final InputSplitProvider splitProvider;
	
	private final Map<String, Future<Path>> distCacheEntries;

	private final ResultPartitionWriter[] writers;
	private final InputGate[] inputGates;
	
	private final ActorRef jobManagerActor;

	private final BlobService blobCache;
	
	// ------------------------------------------------------------------------

	public RuntimeEnvironment(JobID jobId, JobVertexID jobVertexId, ExecutionAttemptID executionId,
								String taskName, String taskNameWithSubtasks,
								int subtaskIndex, int parallelism,
								Configuration jobConfiguration, Configuration taskConfiguration,
								ClassLoader userCodeClassLoader,
								MemoryManager memManager, IOManager ioManager,
								BroadcastVariableManager bcVarManager,
								InputSplitProvider splitProvider,
								Map<String, Future<Path>> distCacheEntries,
								ResultPartitionWriter[] writers,
								InputGate[] inputGates,
								ActorRef jobManagerActor, BlobService blobCache) {
		
		checkArgument(parallelism > 0 && subtaskIndex >= 0 && subtaskIndex < parallelism);
		
		this.jobId = checkNotNull(jobId);
		this.jobVertexId = checkNotNull(jobVertexId);
		this.executionId = checkNotNull(executionId);
		this.taskName = checkNotNull(taskName);
		this.taskNameWithSubtasks = checkNotNull(taskNameWithSubtasks);
		this.subtaskIndex = subtaskIndex;
		this.parallelism = parallelism;
		this.jobConfiguration = checkNotNull(jobConfiguration);
		this.taskConfiguration = checkNotNull(taskConfiguration);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.memManager = checkNotNull(memManager);
		this.ioManager = checkNotNull(ioManager);
		this.bcVarManager = checkNotNull(bcVarManager);
		this.splitProvider = checkNotNull(splitProvider);
		this.distCacheEntries = checkNotNull(distCacheEntries);
		this.writers = checkNotNull(writers);
		this.inputGates = checkNotNull(inputGates);
		this.jobManagerActor = checkNotNull(jobManagerActor);
		this.blobCache = checkNotNull(blobCache);
	}


	// ------------------------------------------------------------------------
	
	@Override
	public JobID getJobID() {
		return jobId;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	@Override
	public String getTaskName() {
		return taskName;
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return taskNameWithSubtasks;
	}

	@Override
	public int getNumberOfSubtasks() {
		return parallelism;
	}

	@Override
	public int getIndexInSubtaskGroup() {
		return subtaskIndex;
	}

	@Override
	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	@Override
	public Configuration getTaskConfiguration() {
		return taskConfiguration;
	}
	
	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public BlobService getBlobCache() {
		return this.blobCache;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return memManager;
	}

	@Override
	public IOManager getIOManager() {
		return ioManager;
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return bcVarManager;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return splitProvider;
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return distCacheEntries;
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		return writers[index];
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return writers;
	}

	@Override
	public InputGate getInputGate(int index) {
		return inputGates[index];
	}

	@Override
	public InputGate[] getAllInputGates() {
		return inputGates;
	}

	@Override
	public void reportAccumulators(Map<String, Accumulator<?, ?>> accumulators) {
		Map<String, byte[]> ser;
		try {
			ser = serializeAccumulators(accumulators);
		} catch (IOException e) {
			throw new RuntimeException("Cannot serialize accumulators", e);
		}

		// todo I have to define a parameter in the configuration file different from the akka.framesize
		// and put there the threshold value, above which an Accumulator will be considered oversized
		// and be sent through the BlobCache.
		// Here the threshold should be the minimum between the two, i.e. akka.framesize and that
		// parameter.

		if(isAccumulatorTooLarge(ser, (long) (0.75 * AkkaUtils.getFramesize(jobConfiguration)))) {

			Map<String, List<BlobKey>> accumulatorsToBlobRefs;
			try {
				accumulatorsToBlobRefs = putAccumulatorBlobsToBlobCache(this, ser);
			} catch (IOException e) {
				throw new RuntimeException("Error while sending the Accumulators to the BlobCache: "+ e.getMessage());
			}

			LargeAccumulatorEvent evt;
			try {
				evt = new LargeAccumulatorEvent(getJobID(), accumulatorsToBlobRefs);
			} catch (IOException e) {
				throw new RuntimeException("Cannot serialize large accumulator references to send them to JobManager", e);
			}

			ReportLargeAccumulatorResult largeResult = new ReportLargeAccumulatorResult(jobId, executionId, evt);
			jobManagerActor.tell(largeResult, ActorRef.noSender());
		} else {

			SmallAccumulatorEvent evt;
			try {
				evt = new SmallAccumulatorEvent(getJobID(), accumulators);
			} catch (IOException e) {
				throw new RuntimeException("Cannot serialize accumulator contents to send them to JobManager", e);
			}

			ReportSmallAccumulatorResult accResult = new ReportSmallAccumulatorResult(jobId, executionId, evt);
			jobManagerActor.tell(accResult, ActorRef.noSender());
		}
	}

	/**
	 * Checks if the serialized Accumulators are larger then <code>thresholdInBytes</code>.
	 * @param serializedAccums the blobs refering to the accumulators to be sent.
	 * @param threshold the threshold above which a message is considered oversized (in Bytes).
	 * @return <code>true</code> if the message is oversized, <code>false</code> otherwise.
	 * */
	private boolean isAccumulatorTooLarge(Map<String, byte[]> serializedAccums, long threshold) {
		int totalSize = 0;
		for(byte[] msg : serializedAccums.values())
			totalSize += msg.length;
		return  totalSize > threshold;
	}

	/**
	 * Puts the blobs of the large accumulators on the BlobCache.
	 * @param env the context in which the task is executed.
	 * @param accumulatorBlobs the blobs to be stored in the cache.
	 * @return the name of each accumulator with the BlobKey that identifies its blob in the BlobCache.
	 * @throws IOException if there is an error when connecting to the BlobServer or when sending the data.
	 * */
	private static Map<String, List<BlobKey>> putAccumulatorBlobsToBlobCache(Environment env, Map<String, byte[]> accumulatorBlobs) throws IOException {
		if (accumulatorBlobs.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, List<BlobKey>> keys = new HashMap<String, List<BlobKey>>();
		BlobClient bc = new BlobClient(env.getBlobCache().getBlobServerAddress());
		for(String key: accumulatorBlobs.keySet()) {
			BlobKey blobKey = bc.put(accumulatorBlobs.get(key));
			List<BlobKey> accKeys = keys.get(key);
			if(accKeys == null){
				accKeys = new ArrayList<BlobKey>();
			}
			accKeys.add(blobKey);
			keys.put(key, accKeys);
		}
		bc.close();
		return keys;
	}

	/**
	 * Serializes the Accumulator (the Object, not only the local value). The result will be stored in
	 * the BlobCache. We serialize the full object, as this will have to fetched by the Client from the
	 * BlobCache, and merged with the rest of the (partial) Accumulators.
	 *
	 * @param accumulators the accumulators to be serialized.
	 *
	 * @return the blobs containing the serialized accumulators with their keys.
	 * */
	private Map<String, byte[]> serializeAccumulators(Map<String, Accumulator<?, ?>> accumulators) throws IOException {
		Map<String, byte[]> blobs = new HashMap<String, byte[]>();
		for(Map.Entry<String, Accumulator<?, ?>> entry : accumulators.entrySet()) {
			String key = entry.getKey();
			Accumulator<?, ?> accumulator = entry.getValue();
			byte[] serAcc = InstantiationUtil.serializeObject(accumulator);
			blobs.put(key, serAcc);
		}
		return blobs;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId) {
		acknowledgeCheckpoint(checkpointId, null);
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
		// try and create a serialized version of the state handle
		SerializedValue<StateHandle<?>> serializedState;
		if (state == null) {
			serializedState = null;
		} else {
			try {
				serializedState = new SerializedValue<StateHandle<?>>(state);
			} catch (Exception e) {
				throw new RuntimeException("Failed to serialize state handle during checkpoint confirmation", e);
			}
		}
		
		AcknowledgeCheckpoint message = new AcknowledgeCheckpoint(jobId, executionId, checkpointId, serializedState);
		jobManagerActor.tell(message, ActorRef.noSender());
	}
}
