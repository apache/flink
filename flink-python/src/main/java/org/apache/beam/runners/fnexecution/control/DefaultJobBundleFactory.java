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
package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ExternalEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.NoopLock;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link JobBundleFactory} for which the implementation can specify a custom {@link
 * EnvironmentFactory} for environment management. Note that returned {@link StageBundleFactory
 * stage bundle factories} are not thread-safe. Instead, a new stage factory should be created for
 * each client. {@link DefaultJobBundleFactory} initializes the Environment lazily when the forStage
 * is called for a stage.
 */
@ThreadSafe
@SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DefaultJobBundleFactory implements JobBundleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobBundleFactory.class);
    private static final IdGenerator factoryIdGenerator = IdGenerators.incrementingLongs();

    private final String factoryId = factoryIdGenerator.getId();
    private final ImmutableList<EnvironmentCacheAndLock> environmentCaches;
    private final AtomicInteger stageBundleFactoryCount = new AtomicInteger();
    private final Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap;
    private final ExecutorService executor;
    private final MapControlClientPool clientPool;
    private final IdGenerator stageIdGenerator;
    private final int environmentExpirationMillis;
    private final Semaphore availableCachesSemaphore;
    private final LinkedBlockingDeque<EnvironmentCacheAndLock> availableCaches;
    private final boolean loadBalanceBundles;
    /**
     * Clients which were evicted due to environment expiration but still had pending references.
     */
    private final Set<WrappedSdkHarnessClient> evictedActiveClients;

    private boolean closed;

    public static DefaultJobBundleFactory create(JobInfo jobInfo) {
        PipelineOptions pipelineOptions =
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
        Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap =
                ImmutableMap.of(
                        BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER),
                        new DockerEnvironmentFactory.Provider(pipelineOptions),
                        BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS),
                        new ProcessEnvironmentFactory.Provider(pipelineOptions),
                        BeamUrns.getUrn(StandardEnvironments.Environments.EXTERNAL),
                        new ExternalEnvironmentFactory.Provider(),
                        Environments.ENVIRONMENT_EMBEDDED, // Non Public urn for testing.
                        new EmbeddedEnvironmentFactory.Provider(pipelineOptions));
        return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
    }

    public static DefaultJobBundleFactory create(
            JobInfo jobInfo,
            Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap) {
        return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
    }

    DefaultJobBundleFactory(
            JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryMap) {
        IdGenerator stageIdSuffixGenerator = IdGenerators.incrementingLongs();
        this.environmentFactoryProviderMap = environmentFactoryMap;
        this.executor = Executors.newCachedThreadPool();
        this.clientPool = MapControlClientPool.create();
        this.stageIdGenerator = () -> factoryId + "-" + stageIdSuffixGenerator.getId();
        this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
        this.loadBalanceBundles = shouldLoadBalanceBundles(jobInfo);
        this.environmentCaches =
                createEnvironmentCaches(
                        serverFactory -> createServerInfo(jobInfo, serverFactory),
                        getMaxEnvironmentClients(jobInfo));
        this.availableCachesSemaphore = new Semaphore(environmentCaches.size(), true);
        this.availableCaches = new LinkedBlockingDeque<>(environmentCaches);
        this.evictedActiveClients = Sets.newConcurrentHashSet();
    }

    @VisibleForTesting
    DefaultJobBundleFactory(
            JobInfo jobInfo,
            Map<String, EnvironmentFactory.Provider> environmentFactoryMap,
            IdGenerator stageIdGenerator,
            ServerInfo serverInfo) {
        this.environmentFactoryProviderMap = environmentFactoryMap;
        this.executor = Executors.newCachedThreadPool();
        this.clientPool = MapControlClientPool.create();
        this.stageIdGenerator = stageIdGenerator;
        this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
        this.loadBalanceBundles = shouldLoadBalanceBundles(jobInfo);
        this.environmentCaches =
                createEnvironmentCaches(
                        serverFactory -> serverInfo, getMaxEnvironmentClients(jobInfo));
        this.availableCachesSemaphore = new Semaphore(environmentCaches.size(), true);
        this.availableCaches = new LinkedBlockingDeque<>(environmentCaches);
        this.evictedActiveClients = Sets.newConcurrentHashSet();
    }

    private static class EnvironmentCacheAndLock {
        final Lock lock;
        final LoadingCache<Environment, WrappedSdkHarnessClient> cache;

        EnvironmentCacheAndLock(
                LoadingCache<Environment, WrappedSdkHarnessClient> cache, Lock lock) {
            this.lock = lock;
            this.cache = cache;
        }
    }

    private ImmutableList<EnvironmentCacheAndLock> createEnvironmentCaches(
            ThrowingFunction<ServerFactory, ServerInfo> serverInfoCreator, int count) {

        ImmutableList.Builder<EnvironmentCacheAndLock> caches = ImmutableList.builder();
        for (int i = 0; i < count; i++) {

            final Lock refLock;
            if (environmentExpirationMillis > 0) {
                // The lock ensures there is no race condition between expiring an environment and a
                // client
                // still attempting to use it, hence referencing it.
                refLock = new ReentrantLock(true);
            } else {
                refLock = NoopLock.get();
            }

            CacheBuilder<Environment, WrappedSdkHarnessClient> cacheBuilder =
                    CacheBuilder.newBuilder()
                            .removalListener(
                                    notification -> {
                                        WrappedSdkHarnessClient client = notification.getValue();
                                        final int refCount;
                                        // We need to use a lock here to ensure we are not causing
                                        // the environment to
                                        // be removed if beforehand a StageBundleFactory has
                                        // retrieved it but not yet
                                        // issued ref() on it.
                                        refLock.lock();
                                        try {
                                            refCount = client.unref();
                                        } finally {
                                            refLock.unlock();
                                        }
                                        if (refCount > 0) {
                                            LOG.warn(
                                                    "Expiring environment {} with {} remaining bundle references. Taking note to clean it up during shutdown if the references are not removed by then.",
                                                    notification.getKey(),
                                                    refCount);
                                            evictedActiveClients.add(client);
                                        }
                                    });

            if (environmentExpirationMillis > 0) {
                cacheBuilder.expireAfterWrite(environmentExpirationMillis, TimeUnit.MILLISECONDS);
            }

            LoadingCache<Environment, WrappedSdkHarnessClient> cache =
                    cacheBuilder.build(
                            new CacheLoader<Environment, WrappedSdkHarnessClient>() {

                                private void close(ServerInfo serverInfo) {
                                    try (AutoCloseable provisioningServer =
                                                    serverInfo.getProvisioningServer();
                                            AutoCloseable retrievalServer =
                                                    serverInfo.getRetrievalServer();
                                            AutoCloseable stateServer =
                                                    serverInfo.getStateServer();
                                            AutoCloseable dataServer = serverInfo.getDataServer();
                                            AutoCloseable controlServer =
                                                    serverInfo.getControlServer();
                                            // Close the logging server first to prevent spaming the
                                            // logs with error messages
                                            AutoCloseable loggingServer =
                                                    serverInfo.getLoggingServer()) {
                                    } catch (Exception e) {
                                        LOG.warn("Error cleaning up servers {}", serverInfo, e);
                                    }
                                }

                                @Override
                                public WrappedSdkHarnessClient load(Environment environment)
                                        throws Exception {
                                    EnvironmentFactory.Provider environmentFactoryProvider =
                                            environmentFactoryProviderMap.get(environment.getUrn());
                                    ServerFactory serverFactory =
                                            environmentFactoryProvider.getServerFactory();
                                    ServerInfo serverInfo = serverInfoCreator.apply(serverFactory);
                                    String workerId = stageIdGenerator.getId();
                                    serverInfo
                                            .getProvisioningServer()
                                            .getService()
                                            .registerEnvironment(workerId, environment);
                                    EnvironmentFactory environmentFactory =
                                            environmentFactoryProvider.createEnvironmentFactory(
                                                    serverInfo.getControlServer(),
                                                    serverInfo.getLoggingServer(),
                                                    serverInfo.getRetrievalServer(),
                                                    serverInfo.getProvisioningServer(),
                                                    clientPool,
                                                    stageIdGenerator);
                                    try {
                                        return WrappedSdkHarnessClient.wrapping(
                                                environmentFactory.createEnvironment(
                                                        environment, workerId),
                                                serverInfo);
                                    } catch (Exception e) {
                                        close(serverInfo);
                                        throw e;
                                    }
                                }
                            });

            caches.add(new EnvironmentCacheAndLock(cache, refLock));
        }
        return caches.build();
    }

    private static int getEnvironmentExpirationMillis(JobInfo jobInfo) {
        PipelineOptions pipelineOptions =
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
        return pipelineOptions.as(PortablePipelineOptions.class).getEnvironmentExpirationMillis();
    }

    private static int getMaxEnvironmentClients(JobInfo jobInfo) {
        PortablePipelineOptions pipelineOptions =
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
                        .as(PortablePipelineOptions.class);
        int maxEnvironments =
                MoreObjects.firstNonNull(pipelineOptions.getSdkWorkerParallelism(), 1);
        Preconditions.checkArgument(maxEnvironments >= 0, "sdk_worker_parallelism must be >= 0");
        if (maxEnvironments == 0) {
            // if this is 0, use the auto behavior of num_cores - 1 so that we leave some resources
            // available for the java process
            maxEnvironments = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
        }
        return maxEnvironments;
    }

    private static boolean shouldLoadBalanceBundles(JobInfo jobInfo) {
        PipelineOptions pipelineOptions =
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
        boolean loadBalanceBundles =
                pipelineOptions.as(PortablePipelineOptions.class).getLoadBalanceBundles();
        if (loadBalanceBundles) {
            int stateCacheSize =
                    Integer.parseInt(
                            MoreObjects.firstNonNull(
                                    ExperimentalOptions.getExperimentValue(
                                            pipelineOptions, ExperimentalOptions.STATE_CACHE_SIZE),
                                    "0"));
            Preconditions.checkArgument(
                    stateCacheSize == 0,
                    "%s must be 0 when using bundle load balancing",
                    ExperimentalOptions.STATE_CACHE_SIZE);
        }
        return loadBalanceBundles;
    }

    @Override
    public StageBundleFactory forStage(ExecutableStage executableStage) {
        return new SimpleStageBundleFactory(executableStage);
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        // The following code is written defensively to guard against any exceptions occurring
        // during shutdown. It is not visually appealing but unless it can be written more
        // defensively, there is no reason to change it.
        Exception exception = null;
        for (EnvironmentCacheAndLock environmentCache : environmentCaches) {
            try {
                // Clear the cache. This closes all active environments.
                // note this may cause open calls to be cancelled by the peer
                environmentCache.cache.invalidateAll();
                environmentCache.cache.cleanUp();
            } catch (Exception e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            }
        }
        // Cleanup any left-over environments which were not properly dereferenced by the Runner,
        // e.g.
        // when the bundle was not closed properly. This ensures we do not leak resources.
        for (WrappedSdkHarnessClient client : evictedActiveClients) {
            try {
                //noinspection StatementWithEmptyBody
                while (client.unref() > 0) {
                    // Remove any pending references from the client to force closing the
                    // environment
                }
            } catch (Exception e) {
                if (exception != null) {
                    exception.addSuppressed(e);
                } else {
                    exception = e;
                }
            }
        }
        try {
            executor.shutdown();
        } catch (Exception e) {
            if (exception != null) {
                exception.addSuppressed(e);
            } else {
                exception = e;
            }
        }
        closed = true;
        if (exception != null) {
            throw exception;
        }
    }

    private static Map<String, RemoteOutputReceiver<?>> getOutputReceivers(
            ExecutableProcessBundleDescriptor processBundleDescriptor,
            OutputReceiverFactory outputReceiverFactory) {
        ImmutableMap.Builder<String, RemoteOutputReceiver<?>> outputReceivers =
                ImmutableMap.builder();
        for (Map.Entry<String, Coder> remoteOutputCoder :
                processBundleDescriptor.getRemoteOutputCoders().entrySet()) {
            String outputTransform = remoteOutputCoder.getKey();
            Coder coder = remoteOutputCoder.getValue();
            String bundleOutputPCollection =
                    Iterables.getOnlyElement(
                            processBundleDescriptor
                                    .getProcessBundleDescriptor()
                                    .getTransformsOrThrow(outputTransform)
                                    .getInputsMap()
                                    .values());
            FnDataReceiver outputReceiver = outputReceiverFactory.create(bundleOutputPCollection);
            outputReceivers.put(outputTransform, RemoteOutputReceiver.of(coder, outputReceiver));
        }
        return outputReceivers.build();
    }

    private static Map<KV<String, String>, RemoteOutputReceiver<Timer<?>>> getTimerReceivers(
            ExecutableProcessBundleDescriptor processBundleDescriptor,
            TimerReceiverFactory timerReceiverFactory) {
        ImmutableMap.Builder<KV<String, String>, RemoteOutputReceiver<Timer<?>>> timerReceivers =
                ImmutableMap.builder();
        for (Map.Entry<String, Map<String, TimerSpec>> transformTimerSpecs :
                processBundleDescriptor.getTimerSpecs().entrySet()) {
            for (TimerSpec timerSpec : transformTimerSpecs.getValue().values()) {
                FnDataReceiver<Timer<?>> receiver =
                        (FnDataReceiver)
                                timerReceiverFactory.create(
                                        timerSpec.transformId(), timerSpec.timerId());
                timerReceivers.put(
                        KV.of(timerSpec.transformId(), timerSpec.timerId()),
                        RemoteOutputReceiver.of(timerSpec.coder(), receiver));
            }
        }
        return timerReceivers.build();
    }

    private static class PreparedClient {
        private BundleProcessor processor;
        private ExecutableProcessBundleDescriptor processBundleDescriptor;
        private WrappedSdkHarnessClient wrappedClient;
    }

    private PreparedClient prepare(
            WrappedSdkHarnessClient wrappedClient, ExecutableStage executableStage) {
        PreparedClient preparedClient = new PreparedClient();
        try {
            preparedClient.wrappedClient = wrappedClient;
            preparedClient.processBundleDescriptor =
                    ProcessBundleDescriptors.fromExecutableStage(
                            stageIdGenerator.getId(),
                            executableStage,
                            wrappedClient.getServerInfo().getDataServer().getApiServiceDescriptor(),
                            wrappedClient
                                    .getServerInfo()
                                    .getStateServer()
                                    .getApiServiceDescriptor());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create ProcessBundleDescriptor.", e);
        }

        preparedClient.processor =
                wrappedClient
                        .getClient()
                        .getProcessor(
                                preparedClient.processBundleDescriptor.getProcessBundleDescriptor(),
                                preparedClient.processBundleDescriptor.getRemoteInputDestinations(),
                                wrappedClient.getServerInfo().getStateServer().getService(),
                                preparedClient.processBundleDescriptor.getTimerSpecs());
        return preparedClient;
    }

    /**
     * A {@link StageBundleFactory} for remotely processing bundles that supports environment
     * expiration.
     */
    private class SimpleStageBundleFactory implements StageBundleFactory {

        private final ExecutableStage executableStage;
        private final int environmentIndex;
        private final IdentityHashMap<WrappedSdkHarnessClient, PreparedClient> preparedClients =
                new IdentityHashMap<>();
        private volatile PreparedClient currentClient;

        private SimpleStageBundleFactory(ExecutableStage executableStage) {
            this.executableStage = executableStage;
            this.environmentIndex =
                    stageBundleFactoryCount.getAndIncrement() % environmentCaches.size();
            WrappedSdkHarnessClient client =
                    environmentCaches
                            .get(environmentIndex)
                            .cache
                            .getUnchecked(executableStage.getEnvironment());
            this.currentClient = prepare(client, executableStage);
            this.preparedClients.put(client, currentClient);
        }

        @Override
        public RemoteBundle getBundle(
                OutputReceiverFactory outputReceiverFactory,
                TimerReceiverFactory timerReceiverFactory,
                StateRequestHandler stateRequestHandler,
                BundleProgressHandler progressHandler,
                BundleFinalizationHandler finalizationHandler,
                BundleCheckpointHandler checkpointHandler)
                throws Exception {
            // TODO: Consider having BundleProcessor#newBundle take in an OutputReceiverFactory
            // rather
            // than constructing the receiver map here. Every bundle factory will need this.

            final EnvironmentCacheAndLock currentCache;
            final WrappedSdkHarnessClient client;
            if (loadBalanceBundles) {
                // The semaphore is used to ensure fairness, i.e. first stop first go.
                availableCachesSemaphore.acquire();
                // The blocking queue of caches for serving multiple bundles concurrently.
                currentCache = availableCaches.take();
                // Lock because the environment expiration can remove the ref for the client
                // which would close the underlying environment before we can ref it.
                currentCache.lock.lock();
                try {
                    client = currentCache.cache.getUnchecked(executableStage.getEnvironment());
                    client.ref();
                } finally {
                    currentCache.lock.unlock();
                }

                currentClient = preparedClients.get(client);
                if (currentClient == null) {
                    // we are using this client for the first time
                    preparedClients.put(client, currentClient = prepare(client, executableStage));
                    // cleanup any expired clients
                    preparedClients.keySet().removeIf(c -> c.bundleRefCount.get() == 0);
                }

            } else {
                currentCache = environmentCaches.get(environmentIndex);
                // Lock because the environment expiration can remove the ref for the client which
                // would
                // close the underlying environment before we can ref it.
                currentCache.lock.lock();
                try {
                    client = currentCache.cache.getUnchecked(executableStage.getEnvironment());
                    client.ref();
                } finally {
                    currentCache.lock.unlock();
                }

                if (currentClient.wrappedClient != client) {
                    // reset after environment expired
                    preparedClients.clear();
                    currentClient = prepare(client, executableStage);
                    preparedClients.put(client, currentClient);
                }
            }

            if (environmentExpirationMillis > 0) {
                // Cleanup list of clients which were active during eviction but now do not hold
                // references
                evictedActiveClients.removeIf(c -> c.bundleRefCount.get() == 0);
            }

            final RemoteBundle bundle =
                    currentClient.processor.newBundle(
                            getOutputReceivers(
                                    currentClient.processBundleDescriptor, outputReceiverFactory),
                            getTimerReceivers(
                                    currentClient.processBundleDescriptor, timerReceiverFactory),
                            stateRequestHandler,
                            progressHandler,
                            finalizationHandler,
                            checkpointHandler);
            return new RemoteBundle() {
                @Override
                public String getId() {
                    return bundle.getId();
                }

                @Override
                public Map<String, FnDataReceiver> getInputReceivers() {
                    return bundle.getInputReceivers();
                }

                @Override
                public Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers() {
                    return bundle.getTimerReceivers();
                }

                @Override
                public void requestProgress() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void split(double fractionOfRemainder) {
                    bundle.split(fractionOfRemainder);
                }

                @Override
                public void close() throws Exception {
                    try {
                        bundle.close();
                    } finally {
                        client.unref();
                        if (loadBalanceBundles) {
                            availableCaches.offer(currentCache);
                            availableCachesSemaphore.release();
                        }
                    }
                }
            };
        }

        @Override
        public ExecutableProcessBundleDescriptor getProcessBundleDescriptor() {
            return currentClient.processBundleDescriptor;
        }

        @Override
        public InstructionRequestHandler getInstructionRequestHandler() {
            return currentClient.wrappedClient.getClient().getInstructionRequestHandler();
        }

        @Override
        public void close() throws Exception {
            // Clear reference to encourage cache eviction. Values are weakly referenced.
            preparedClients.clear();
        }
    }

    /**
     * Holder for an {@link SdkHarnessClient} along with its associated state and data servers. As
     * of now, there is a 1:1 relationship between data services and harness clients. The servers
     * are packaged here to tie server lifetimes to harness client lifetimes.
     */
    protected static class WrappedSdkHarnessClient {

        private final RemoteEnvironment environment;
        private final SdkHarnessClient client;
        private final ServerInfo serverInfo;
        private final AtomicInteger bundleRefCount = new AtomicInteger();

        private boolean closed;

        static WrappedSdkHarnessClient wrapping(
                RemoteEnvironment environment, ServerInfo serverInfo) {
            SdkHarnessClient client =
                    SdkHarnessClient.usingFnApiClient(
                            environment.getInstructionRequestHandler(),
                            serverInfo.getDataServer().getService());
            return new WrappedSdkHarnessClient(environment, client, serverInfo);
        }

        private WrappedSdkHarnessClient(
                RemoteEnvironment environment, SdkHarnessClient client, ServerInfo serverInfo) {
            this.environment = environment;
            this.client = client;
            this.serverInfo = serverInfo;
            ref();
        }

        SdkHarnessClient getClient() {
            return client;
        }

        ServerInfo getServerInfo() {
            return serverInfo;
        }

        public synchronized void close() {
            if (closed) {
                return;
            }
            // DO NOT ADD ANYTHING HERE WHICH MIGHT CAUSE THE BLOCK BELOW TO NOT BE EXECUTED.
            // If we exit prematurely (e.g. due to an exception), resources won't be cleaned up
            // properly.
            // Please make an AutoCloseable and add it to the try statement below.
            // These will be closed in the reverse creation order:
            try (AutoCloseable envCloser = environment;
                    AutoCloseable provisioningServer = serverInfo.getProvisioningServer();
                    AutoCloseable retrievalServer = serverInfo.getRetrievalServer();
                    AutoCloseable stateServer = serverInfo.getStateServer();
                    AutoCloseable dataServer = serverInfo.getDataServer();
                    AutoCloseable controlServer = serverInfo.getControlServer();
                    // Close the logging server first to prevent spaming the logs with error
                    // messages
                    AutoCloseable loggingServer = serverInfo.getLoggingServer()) {
                // Wrap resources in try-with-resources to ensure all are cleaned up.
                // This will close _all_ of these even in the presence of exceptions.
                // The first exception encountered will be the base exception,
                // the next one will be added via Throwable#addSuppressed.
                closed = true;
            } catch (Exception e) {
                LOG.warn("Error cleaning up servers {}", environment.getEnvironment(), e);
            }
            // TODO: Wait for executor shutdown?
        }

        private int ref() {
            return bundleRefCount.incrementAndGet();
        }

        private int unref() {
            int refCount = bundleRefCount.decrementAndGet();
            Preconditions.checkState(refCount >= 0, "Reference count must not be negative.");
            if (refCount == 0) {
                // Close environment after it was removed from cache and all bundles finished.
                LOG.info("Closing environment {}", environment.getEnvironment());
                close();
            }
            return refCount;
        }
    }

    private ServerInfo createServerInfo(JobInfo jobInfo, ServerFactory serverFactory)
            throws IOException {
        Preconditions.checkNotNull(serverFactory, "serverFactory can not be null");

        PortablePipelineOptions portableOptions =
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
                        .as(PortablePipelineOptions.class);

        GrpcFnServer<FnApiControlClientPoolService> controlServer =
                GrpcFnServer.allocatePortAndCreateFor(
                        FnApiControlClientPoolService.offeringClientsToPool(
                                clientPool.getSink(),
                                GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
                        serverFactory);
        GrpcFnServer<GrpcLoggingService> loggingServer =
                GrpcFnServer.allocatePortAndCreateFor(
                        GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
        GrpcFnServer<ArtifactRetrievalService> retrievalServer =
                GrpcFnServer.allocatePortAndCreateFor(
                        new ArtifactRetrievalService(), serverFactory);
        ProvisionApi.ProvisionInfo.Builder provisionInfo = jobInfo.toProvisionInfo().toBuilder();
        provisionInfo.setLoggingEndpoint(loggingServer.getApiServiceDescriptor());
        provisionInfo.setArtifactEndpoint(retrievalServer.getApiServiceDescriptor());
        provisionInfo.setControlEndpoint(controlServer.getApiServiceDescriptor());
        GrpcFnServer<StaticGrpcProvisionService> provisioningServer =
                GrpcFnServer.allocatePortAndCreateFor(
                        StaticGrpcProvisionService.create(
                                provisionInfo.build(),
                                GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
                        serverFactory);
        GrpcFnServer<GrpcDataService> dataServer =
                GrpcFnServer.allocatePortAndCreateFor(
                        GrpcDataService.create(
                                portableOptions, executor, OutboundObserverFactory.serverDirect()),
                        serverFactory);
        GrpcFnServer<GrpcStateService> stateServer =
                GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

        ServerInfo serverInfo =
                new AutoValue_DefaultJobBundleFactory_ServerInfo.Builder()
                        .setControlServer(controlServer)
                        .setLoggingServer(loggingServer)
                        .setRetrievalServer(retrievalServer)
                        .setProvisioningServer(provisioningServer)
                        .setDataServer(dataServer)
                        .setStateServer(stateServer)
                        .build();
        return serverInfo;
    }

    /** A container for EnvironmentFactory and its corresponding Grpc servers. */
    public abstract static class ServerInfo {
        abstract GrpcFnServer<FnApiControlClientPoolService> getControlServer();

        abstract GrpcFnServer<GrpcLoggingService> getLoggingServer();

        abstract GrpcFnServer<ArtifactRetrievalService> getRetrievalServer();

        abstract GrpcFnServer<StaticGrpcProvisionService> getProvisioningServer();

        abstract GrpcFnServer<GrpcDataService> getDataServer();

        abstract GrpcFnServer<GrpcStateService> getStateServer();

        abstract Builder toBuilder();

        abstract static class Builder {
            abstract Builder setControlServer(GrpcFnServer<FnApiControlClientPoolService> server);

            abstract Builder setLoggingServer(GrpcFnServer<GrpcLoggingService> server);

            abstract Builder setRetrievalServer(GrpcFnServer<ArtifactRetrievalService> server);

            abstract Builder setProvisioningServer(GrpcFnServer<StaticGrpcProvisionService> server);

            abstract Builder setDataServer(GrpcFnServer<GrpcDataService> server);

            abstract Builder setStateServer(GrpcFnServer<GrpcStateService> server);

            abstract ServerInfo build();
        }
    }
}
