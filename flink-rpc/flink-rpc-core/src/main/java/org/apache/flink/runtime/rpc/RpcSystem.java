/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.exceptions.RpcLoaderException;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.ServiceLoader;

/**
 * This interface serves as a factory interface for RPC services, with some additional utilities
 * that are reliant on implementation details of the RPC service.
 */
public interface RpcSystem extends RpcSystemUtils, AutoCloseable {

    /**
     * Returns a builder for an {@link RpcService} that is only reachable from the local machine.
     *
     * @param configuration Flink configuration
     * @return rpc service builder
     */
    RpcServiceBuilder localServiceBuilder(Configuration configuration);

    /**
     * Returns a builder for an {@link RpcService} that is reachable from other machines.
     *
     * @param configuration Flink configuration
     * @param externalAddress optional address under which the RpcService should be reachable
     * @param externalPortRange port range from which 1 port will be chosen under which the
     *     RpcService should be reachable
     * @return rpc service builder
     */
    RpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange);

    /** Hook to cleanup resources, like common thread pools or classloaders. */
    @Override
    default void close() {}

    /** Builder for {@link RpcService}. */
    interface RpcServiceBuilder {
        RpcServiceBuilder withComponentName(String name);

        RpcServiceBuilder withBindAddress(String bindAddress);

        RpcServiceBuilder withBindPort(int bindPort);

        RpcServiceBuilder withExecutorConfiguration(
                FixedThreadPoolExecutorConfiguration executorConfiguration);

        RpcServiceBuilder withExecutorConfiguration(
                ForkJoinExecutorConfiguration executorConfiguration);

        RpcService createAndStart() throws Exception;
    }

    /**
     * Loads the RpcSystem.
     *
     * @return loaded RpcSystem
     */
    static RpcSystem load() {
        return load(new Configuration());
    }

    /**
     * Loads the RpcSystem.
     *
     * @param config Flink configuration
     * @return loaded RpcSystem
     */
    static RpcSystem load(Configuration config) {
        final PriorityQueue<RpcSystemLoader> rpcSystemLoaders =
                new PriorityQueue<>(Comparator.comparingInt(RpcSystemLoader::getLoadPriority));
        ServiceLoader.load(RpcSystemLoader.class).forEach(rpcSystemLoaders::add);

        final Iterator<RpcSystemLoader> iterator = rpcSystemLoaders.iterator();

        Exception loadError = null;
        while (iterator.hasNext()) {
            final RpcSystemLoader next = iterator.next();
            try {
                return next.loadRpcSystem(config);
            } catch (Exception e) {
                loadError = ExceptionUtils.firstOrSuppressed(e, loadError);
            }
        }
        throw new RpcLoaderException("Could not load RpcSystem.", loadError);
    }

    /** Descriptor for creating a fork-join thread-pool. */
    class ForkJoinExecutorConfiguration {

        private final double parallelismFactor;

        private final int minParallelism;

        private final int maxParallelism;

        public ForkJoinExecutorConfiguration(
                double parallelismFactor, int minParallelism, int maxParallelism) {
            this.parallelismFactor = parallelismFactor;
            this.minParallelism = minParallelism;
            this.maxParallelism = maxParallelism;
        }

        public double getParallelismFactor() {
            return parallelismFactor;
        }

        public int getMinParallelism() {
            return minParallelism;
        }

        public int getMaxParallelism() {
            return maxParallelism;
        }
    }

    /** Descriptor for creating a thread-pool with a fixed number of threads. */
    class FixedThreadPoolExecutorConfiguration {

        private final int minNumThreads;

        private final int maxNumThreads;

        private final int threadPriority;

        public FixedThreadPoolExecutorConfiguration(
                int minNumThreads, int maxNumThreads, int threadPriority) {
            if (threadPriority < Thread.MIN_PRIORITY || threadPriority > Thread.MAX_PRIORITY) {
                throw new IllegalArgumentException(
                        String.format(
                                "The thread priority must be within (%s, %s) but it was %s.",
                                Thread.MIN_PRIORITY, Thread.MAX_PRIORITY, threadPriority));
            }

            this.minNumThreads = minNumThreads;
            this.maxNumThreads = maxNumThreads;
            this.threadPriority = threadPriority;
        }

        public int getMinNumThreads() {
            return minNumThreads;
        }

        public int getMaxNumThreads() {
            return maxNumThreads;
        }

        public int getThreadPriority() {
            return threadPriority;
        }
    }
}
