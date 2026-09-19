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

package org.apache.flink.streaming.api.operators.python.process;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.functions.python.PythonEnv;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractExternalPythonFunctionOperatorTest {

    @Test
    void testCloseCancelsRunnerAfterStoppingFlushExecutor() throws Exception {
        final StreamTask<?, ?> containingTask = org.mockito.Mockito.mock(StreamTask.class);
        org.mockito.Mockito.when(containingTask.isCanceled()).thenReturn(true);
        final ExecutorService flushThreadPool = Executors.newSingleThreadExecutor();
        final TestingPythonFunctionRunner functionRunner =
                new TestingPythonFunctionRunner(flushThreadPool);
        final TestingExternalPythonFunctionOperator operator =
                createOperator(containingTask, functionRunner, flushThreadPool);

        try {
            operator.close();

            assertThat(functionRunner.cancelCalled).isTrue();
            assertThat(functionRunner.closeCalled).isFalse();
            assertThat(functionRunner.flushExecutorStoppedBeforeCancel).isTrue();
            assertThat(flushThreadPool.isShutdown()).isTrue();
        } finally {
            flushThreadPool.shutdownNow();
        }
    }

    @Test
    void testCloseClosesRunnerGracefullyForNormalCompletion() throws Exception {
        final StreamTask<?, ?> containingTask = org.mockito.Mockito.mock(StreamTask.class);
        final ExecutorService flushThreadPool = Executors.newSingleThreadExecutor();
        final TestingPythonFunctionRunner functionRunner =
                new TestingPythonFunctionRunner(flushThreadPool);
        final TestingExternalPythonFunctionOperator operator =
                createOperator(containingTask, functionRunner, flushThreadPool);

        try {
            operator.close();

            assertThat(functionRunner.closeCalled).isTrue();
            assertThat(functionRunner.cancelCalled).isFalse();
            assertThat(flushThreadPool.isShutdown()).isTrue();
        } finally {
            flushThreadPool.shutdownNow();
        }
    }

    private static TestingExternalPythonFunctionOperator createOperator(
            StreamTask<?, ?> containingTask,
            TestingPythonFunctionRunner functionRunner,
            ExecutorService flushThreadPool)
            throws ReflectiveOperationException {
        final TestingExternalPythonFunctionOperator operator =
                new TestingExternalPythonFunctionOperator(functionRunner);
        setField(AbstractStreamOperator.class, operator, "container", containingTask);
        setField(
                AbstractExternalPythonFunctionOperator.class,
                operator,
                "flushThreadPool",
                flushThreadPool);
        return operator;
    }

    private static void setField(Class<?> owner, Object target, String fieldName, Object value)
            throws ReflectiveOperationException {
        final Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class TestingExternalPythonFunctionOperator
            extends AbstractExternalPythonFunctionOperator<Object> {

        private final PythonFunctionRunner functionRunner;

        private TestingExternalPythonFunctionOperator(PythonFunctionRunner functionRunner) {
            super(new Configuration());
            this.functionRunner = functionRunner;
            this.pythonFunctionRunner = functionRunner;
        }

        @Override
        public PythonEnv getPythonEnv() {
            return new PythonEnv(PythonEnv.ExecType.PROCESS);
        }

        @Override
        public void emitResult(Tuple3<String, byte[], Integer> resultTuple) {}

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() {
            return functionRunner;
        }
    }

    private static class TestingPythonFunctionRunner implements PythonFunctionRunner {

        private final ExecutorService flushThreadPool;
        private final AtomicBoolean closeCalled = new AtomicBoolean();
        private final AtomicBoolean cancelCalled = new AtomicBoolean();
        private final AtomicBoolean flushExecutorStoppedBeforeCancel = new AtomicBoolean();

        private TestingPythonFunctionRunner(ExecutorService flushThreadPool) {
            this.flushThreadPool = flushThreadPool;
        }

        @Override
        public void open(org.apache.flink.configuration.ReadableConfig config) {}

        @Override
        public void close() {
            closeCalled.set(true);
        }

        @Override
        public void cancel() {
            flushExecutorStoppedBeforeCancel.set(flushThreadPool.isShutdown());
            cancelCalled.set(true);
        }

        @Override
        public void process(byte[] data) {}

        @Override
        public void processTimer(byte[] timerData) {}

        @Override
        public void drainUnregisteredTimers() {}

        @Override
        public Tuple3<String, byte[], Integer> pollResult() {
            return null;
        }

        @Override
        public Tuple3<String, byte[], Integer> takeResult() {
            return null;
        }

        @Override
        public void flush() {}
    }
}
