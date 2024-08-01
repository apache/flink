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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper class that encapsulates a serialized StreamGraph along with its necessary dependencies
 * like JobID, user JAR blob keys, and classpaths.
 *
 * <p>This class is used to transfer the serialized StreamGraph and its dependencies to a cluster,
 * where the necessary UserClassLoader can be reconstructed and the StreamGraph can be deserialized
 * and used.
 */
@Internal
public class StreamGraphWrapper implements ExecutionPlan {

    private final SerializedValue<StreamGraph> serializedStreamGraph;

    private final long initialClientHeartbeatTimeout;

    /**
     * Serialized representation of operator factories.
     *
     * <p>We serialize operator factories separately from the StreamGraph. This separation enables
     * us to perform the serialization of operator factories in parallel, which accelerates the
     * serialization process.
     */
    private final Map<Integer, SerializedValue<StreamOperatorFactory<?>>>
            streamNodeToSerializedOperatorFactories = new HashMap<>();

    private final JobID jobId;

    private final String jobName;

    private final List<PermanentBlobKey> userJarBlobKeys;

    private final List<URL> classpath;

    private final boolean isPartialResourceConfigured;

    private final boolean isEmptyGraph;

    private final JobType jobType;

    private final boolean dynamic;

    private final JobCheckpointingSettings checkpointingSettings;

    private final Configuration jobConfiguration;

    private final List<Path> userJars;

    private final Map<String, DistributedCache.DistributedCacheEntry> userArtifacts;

    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    private SavepointRestoreSettings savepointRestoreSettings;

    private final int maximumParallelism;

    /**
     * Constructs a new StreamGraphWrapper instance.
     *
     * @param streamGraph the StreamGraph to be serialized and transferred
     */
    public StreamGraphWrapper(StreamGraph streamGraph, Executor serializationExecutor)
            throws Exception {
        this.jobId = checkNotNull(streamGraph.getJobId());
        this.jobName = checkNotNull(streamGraph.getJobName());
        this.userJarBlobKeys = checkNotNull(streamGraph.getUserJarBlobKeys());
        this.classpath = checkNotNull(streamGraph.getClasspath());

        checkNotNull(streamGraph);
        checkNotNull(serializationExecutor);
        this.serializedStreamGraph = serializeStreamGraph(streamGraph, serializationExecutor);

        this.isPartialResourceConfigured = isPartialResourceConfigured(streamGraph);
        this.initialClientHeartbeatTimeout = streamGraph.getInitialClientHeartbeatTimeout();
        this.isEmptyGraph = streamGraph.getStreamNodes().isEmpty();
        this.jobType = streamGraph.getJobType();
        this.dynamic = streamGraph.isDynamic();
        this.checkpointingSettings = streamGraph.getJobCheckpointingSettings();
        this.jobConfiguration = streamGraph.getJobConfiguration();
        this.userJars = streamGraph.getUserJars();
        this.userArtifacts = streamGraph.getUserArtifacts();
        this.savepointRestoreSettings = streamGraph.getSavepointRestoreSettings();
        this.maximumParallelism = streamGraph.getMaximumParallelism();
        this.serializedExecutionConfig = new SerializedValue<>(streamGraph.getExecutionConfig());
    }

    private boolean isPartialResourceConfigured(StreamGraph streamGraph) {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            if (streamNode.getMinResources() == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }

    private SerializedValue<StreamGraph> serializeStreamGraph(
            StreamGraph streamGraph, Executor serializationExecutor) throws Exception {
        // 1. Serialize operator factories in parallel to accelerate serialization.
        CompletableFuture<?> future =
                serializeOperatorFactories(streamGraph.getStreamNodes(), serializationExecutor);

        // 2. Serialize the StreamGraph.
        SerializedValue<StreamGraph> serializedStreamGraph = new SerializedValue<>(streamGraph);

        future.get();

        return serializedStreamGraph;
    }

    /**
     * Returns the JobID associated with the StreamGraph.
     *
     * @return the JobID
     */
    @Override
    public JobID getJobID() {
        return jobId;
    }

    /**
     * Returns the list of PermanentBlobKey pointing to the user JARs.
     *
     * @return the list of PermanentBlobKey
     */
    @Override
    public List<PermanentBlobKey> getUserJarBlobKeys() {
        return userJarBlobKeys;
    }

    @Override
    public List<URL> getClasspaths() {
        return classpath;
    }

    private CompletableFuture<?> serializeOperatorFactories(
            Collection<StreamNode> streamNodes, Executor serializationExecutor) {
        return FutureUtils.combineAll(
                streamNodes.stream()
                        .filter(node -> node.getOperatorFactory() != null)
                        .map(
                                node ->
                                        CompletableFuture.runAsync(
                                                () -> {
                                                    try {
                                                        streamNodeToSerializedOperatorFactories.put(
                                                                node.getId(),
                                                                new SerializedValue<>(
                                                                        node.getOperatorFactory()));
                                                    } catch (IOException e) {
                                                        throw new RuntimeException(
                                                                String.format(
                                                                        "Could not serialize stream node %s",
                                                                        node),
                                                                e);
                                                    }
                                                },
                                                serializationExecutor))
                        .collect(Collectors.toList()));
    }

    public StreamGraph deserializeStreamGraph(
            ClassLoader userClassLoader, Executor deserializationExecutor) throws Exception {
        CompletableFuture<Map<Integer, StreamOperatorFactory<?>>> future =
                deserializeOperators(userClassLoader, deserializationExecutor);

        StreamGraph streamGraph = serializedStreamGraph.deserializeValue(userClassLoader);

        streamGraph.setUserArtifacts(userArtifacts);
        streamGraph.setUserJarBlobKeys(userJarBlobKeys);
        streamGraph.setJobConfiguration(jobConfiguration);
        streamGraph.setSavepointRestoreSettings(savepointRestoreSettings);
        streamGraph.setExecutionConfig(serializedExecutionConfig.deserializeValue(userClassLoader));

        Map<Integer, StreamOperatorFactory<?>> streamNodeToOperatorFactories = future.get();

        streamGraph.getStreamNodes().stream()
                .filter(node -> streamNodeToOperatorFactories.containsKey(node.getId()))
                .forEach(
                        node ->
                                node.setOperatorFactory(
                                        streamNodeToOperatorFactories.get(node.getId())));

        return streamGraph;
    }

    private CompletableFuture<Map<Integer, StreamOperatorFactory<?>>> deserializeOperators(
            ClassLoader userClassLoader, Executor serializationExecutor) {
        final Map<Integer, StreamOperatorFactory<?>> result = new HashMap<>();

        return FutureUtils.combineAll(
                        streamNodeToSerializedOperatorFactories.entrySet().stream()
                                .map(
                                        entry ->
                                                CompletableFuture.runAsync(
                                                        () -> {
                                                            try {
                                                                result.put(
                                                                        entry.getKey(),
                                                                        entry.getValue()
                                                                                .deserializeValue(
                                                                                        userClassLoader));
                                                            } catch (Exception e) {
                                                                throw new RuntimeException(
                                                                        String.format(
                                                                                "Could not deserialize stream node %s",
                                                                                entry.getKey()),
                                                                        e);
                                                            }
                                                        },
                                                        serializationExecutor))
                                .collect(Collectors.toList()))
                .thenApply(ignored -> result);
    }

    @Override
    public String getName() {
        return jobName;
    }

    @Override
    public boolean isPartialResourceConfigured() {
        return isPartialResourceConfigured;
    }

    @Override
    public long getInitialClientHeartbeatTimeout() {
        return initialClientHeartbeatTimeout;
    }

    @Override
    public boolean isEmptyGraph() {
        return isEmptyGraph;
    }

    @Override
    public JobType getJobType() {
        return jobType;
    }

    @Override
    public boolean isDynamic() {
        return dynamic;
    }

    @Override
    public JobCheckpointingSettings getCheckpointingSettings() {
        return checkpointingSettings;
    }

    @Override
    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    @Override
    public List<Path> getUserJars() {
        return userJars;
    }

    @Override
    public Map<String, DistributedCache.DistributedCacheEntry> getUserArtifacts() {
        return userArtifacts;
    }

    @Override
    public void addUserJarBlobKey(PermanentBlobKey key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        if (!userJarBlobKeys.contains(key)) {
            userJarBlobKeys.add(key);
        }
    }

    @Override
    public void setUserArtifactBlobKey(String entryName, PermanentBlobKey blobKey)
            throws IOException {
        byte[] serializedBlobKey;
        serializedBlobKey = InstantiationUtil.serializeObject(blobKey);

        userArtifacts.computeIfPresent(
                entryName,
                (key, originalEntry) ->
                        new DistributedCache.DistributedCacheEntry(
                                originalEntry.filePath,
                                originalEntry.isExecutable,
                                serializedBlobKey,
                                originalEntry.isZipped));
    }

    @Override
    public void writeUserArtifactEntriesToConfiguration() {
        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> userArtifact :
                userArtifacts.entrySet()) {
            DistributedCache.writeFileInfoToConfig(
                    userArtifact.getKey(), userArtifact.getValue(), jobConfiguration);
        }
    }

    @Override
    public SavepointRestoreSettings getSavepointRestoreSettings() {
        return savepointRestoreSettings;
    }

    @Override
    public void setSavepointRestoreSettings(SavepointRestoreSettings settings) {
        this.savepointRestoreSettings = checkNotNull(settings, "Savepoint restore settings");
    }

    @Override
    public int getMaximumParallelism() {
        return maximumParallelism;
    }

    @Override
    public SerializedValue<ExecutionConfig> getSerializedExecutionConfig() {
        return serializedExecutionConfig;
    }

    @Override
    public String toString() {
        return "StreamGraph(jobId: " + jobId + ")";
    }
}
