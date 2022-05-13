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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for the {@link CassandraTupleWriteAheadSink}. */
public class CassandraTupleWriteAheadSinkTest {

    @Test(timeout = 20000)
    public void testAckLoopExitOnException() throws Exception {
        final AtomicReference<Runnable> runnableFuture = new AtomicReference<>();

        final ClusterBuilder clusterBuilder =
                new ClusterBuilder() {
                    private static final long serialVersionUID = 4624400760492936756L;

                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        try {
                            BoundStatement boundStatement = mock(BoundStatement.class);
                            when(boundStatement.setDefaultTimestamp(any(long.class)))
                                    .thenReturn(boundStatement);

                            PreparedStatement preparedStatement = mock(PreparedStatement.class);
                            when(preparedStatement.bind(Matchers.anyVararg()))
                                    .thenReturn(boundStatement);

                            ResultSetFuture future = mock(ResultSetFuture.class);
                            when(future.get())
                                    .thenThrow(new RuntimeException("Expected exception."));

                            doAnswer(
                                            new Answer<Void>() {
                                                @Override
                                                public Void answer(
                                                        InvocationOnMock invocationOnMock)
                                                        throws Throwable {
                                                    synchronized (runnableFuture) {
                                                        runnableFuture.set(
                                                                (((Runnable)
                                                                        invocationOnMock
                                                                                .getArguments()[
                                                                                0])));
                                                        runnableFuture.notifyAll();
                                                    }
                                                    return null;
                                                }
                                            })
                                    .when(future)
                                    .addListener(any(Runnable.class), any(Executor.class));

                            Session session = mock(Session.class);
                            when(session.prepare(anyString())).thenReturn(preparedStatement);
                            when(session.executeAsync(any(BoundStatement.class)))
                                    .thenReturn(future);

                            Cluster cluster = mock(Cluster.class);
                            when(cluster.connect()).thenReturn(session);
                            return cluster;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };

        // Our asynchronous executor thread
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                synchronized (runnableFuture) {
                                    while (runnableFuture.get() == null) {
                                        try {
                                            runnableFuture.wait();
                                        } catch (InterruptedException e) {
                                            // ignore interrupts
                                        }
                                    }
                                }
                                runnableFuture.get().run();
                            }
                        })
                .start();

        CheckpointCommitter cc = mock(CheckpointCommitter.class);
        final CassandraTupleWriteAheadSink<Tuple0> sink =
                new CassandraTupleWriteAheadSink<>(
                        "abc",
                        TupleTypeInfo.of(Tuple0.class).createSerializer(new ExecutionConfig()),
                        clusterBuilder,
                        cc);

        OneInputStreamOperatorTestHarness<Tuple0, Tuple0> harness =
                new OneInputStreamOperatorTestHarness<>(sink);
        harness.getEnvironment().getTaskConfiguration().setBoolean("checkpointing", true);

        harness.setup();
        sink.open();

        // we should leave the loop and return false since we've seen an exception
        assertFalse(sink.sendValues(Collections.singleton(new Tuple0()), 1L, 0L));

        sink.close();
    }
}
