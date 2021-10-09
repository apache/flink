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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/** Test suite for {@link ShuffleServiceLoader} utility. */
public class ShuffleServiceLoaderTest extends TestLogger {

    @Test
    public void testLoadDefaultNettyShuffleServiceFactory() throws FlinkException {
        Configuration configuration = new Configuration();
        ShuffleServiceFactory<?, ?, ?> shuffleServiceFactory =
                ShuffleServiceLoader.loadShuffleServiceFactory(configuration);
        assertThat(
                "Loaded shuffle service factory is not the default netty implementation",
                shuffleServiceFactory,
                instanceOf(NettyShuffleServiceFactory.class));
    }

    @Test
    public void testLoadCustomShuffleServiceFactory() throws FlinkException {
        Configuration configuration = new Configuration();
        configuration.setString(
                SHUFFLE_SERVICE_FACTORY_CLASS,
                "org.apache.flink.runtime.shuffle.ShuffleServiceLoaderTest$CustomShuffleServiceFactory");
        ShuffleServiceFactory<?, ?, ?> shuffleServiceFactory =
                ShuffleServiceLoader.loadShuffleServiceFactory(configuration);
        assertThat(
                "Loaded shuffle service factory is not the custom test implementation",
                shuffleServiceFactory,
                instanceOf(CustomShuffleServiceFactory.class));
    }

    @Test(expected = FlinkException.class)
    public void testLoadShuffleServiceFactoryFailure() throws FlinkException {
        Configuration configuration = new Configuration();
        configuration.setString(
                SHUFFLE_SERVICE_FACTORY_CLASS,
                "org.apache.flink.runtime.shuffle.UnavailableShuffleServiceFactory");
        ShuffleServiceLoader.loadShuffleServiceFactory(configuration);
    }

    /**
     * Stub implementation of {@link ShuffleServiceFactory} to test {@link ShuffleServiceLoader}
     * utility.
     */
    public static class CustomShuffleServiceFactory
            implements ShuffleServiceFactory<
                    ShuffleDescriptor, ResultPartitionWriter, IndexedInputGate> {
        @Override
        public ShuffleMaster<ShuffleDescriptor> createShuffleMaster(
                ShuffleMasterContext shuffleMasterContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> createShuffleEnvironment(
                ShuffleEnvironmentContext shuffleEnvironmentContext) {
            throw new UnsupportedOperationException();
        }
    }
}
