/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

/** The provider of {@link SourceCoordinator}. */
public class SourceCoordinatorProvider<SplitT extends SourceSplit>
        extends RecreateOnResetOperatorCoordinator.Provider {
    private static final long serialVersionUID = -1921681440009738462L;
    private final String operatorName;
    private final Source<?, SplitT, ?> source;
    private final int numWorkerThreads;

    /**
     * Construct the {@link SourceCoordinatorProvider}.
     *
     * @param operatorName the name of the operator.
     * @param operatorID the ID of the operator this coordinator corresponds to.
     * @param source the Source that will be used for this coordinator.
     * @param numWorkerThreads the number of threads the should provide to the SplitEnumerator for
     *     doing async calls. See {@link
     *     org.apache.flink.api.connector.source.SplitEnumeratorContext#callAsync(Callable,
     *     BiConsumer) SplitEnumeratorContext.callAsync()}.
     */
    public SourceCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            Source<?, SplitT, ?> source,
            int numWorkerThreads) {
        super(operatorID);
        this.operatorName = operatorName;
        this.source = source;
        this.numWorkerThreads = numWorkerThreads;
    }

    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        final String coordinatorThreadName = "SourceCoordinator-" + operatorName;
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(coordinatorThreadName, context);

        SimpleVersionedSerializer<SplitT> splitSerializer = source.getSplitSerializer();
        SourceCoordinatorContext<SplitT> sourceCoordinatorContext =
                new SourceCoordinatorContext<>(
                        coordinatorThreadFactory, numWorkerThreads, context, splitSerializer);
        return new SourceCoordinator<>(operatorName, source, sourceCoordinatorContext);
    }

    /** A thread factory class that provides some helper methods. */
    public static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorProvider.class);
        private final String coordinatorThreadName;
        private final ClassLoader cl;
        private final Thread.UncaughtExceptionHandler errorHandler;

        @Nullable private Thread t;

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final OperatorCoordinator.Context context) {
            this(
                    coordinatorThreadName,
                    context.getUserCodeClassloader(),
                    (t, e) -> {
                        LOG.error(
                                "Thread '{}' produced an uncaught exception. Failing the job.",
                                t.getName(),
                                e);
                        context.failJob(e);
                    });
        }

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.cl = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(Runnable r) {
            t = new Thread(r, coordinatorThreadName);
            t.setContextClassLoader(cl);
            t.setUncaughtExceptionHandler(this);
            return t;
        }

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }

        String getCoordinatorThreadName() {
            return coordinatorThreadName;
        }

        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == t;
        }
    }
}
