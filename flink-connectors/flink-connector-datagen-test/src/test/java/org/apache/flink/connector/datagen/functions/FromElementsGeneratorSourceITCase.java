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

package org.apache.flink.connector.datagen.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.generated.User;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@code FromElementsGeneratorSourceITCase}. */
class FromElementsGeneratorSourceITCase extends TestLogger {

    private static final int PARALLELISM = 1;

    @RegisterExtension
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    @Test
    @DisplayName("Produces expected String output")
    void testBasicType() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        String[] data = {"Foo", "bar", "baz"};
        FromElementsGeneratorFunction<String> generatorFunction =
                new FromElementsGeneratorFunction<>("Foo", "bar", "baz");
        DataGeneratorSource<String> dataGeneratorSource =
                new DataGeneratorSource<>(generatorFunction, data.length, Types.STRING);
        DataStream<String> stream =
                env.fromSource(
                        dataGeneratorSource, WatermarkStrategy.noWatermarks(), "generator source");

        List<String> result = stream.executeAndCollect(data.length + 1);
        TypeSerializer<String> serializer = generatorFunction.getSerializer();

        assertThat(serializer).isEqualTo(Types.STRING.createSerializer(new ExecutionConfig()));
        assertThat(result).containsExactly(data);
    }

    @Test
    @DisplayName("Handles Avro data correctly")
    void testAvroType() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        User user1 = new User("Foo", 1);
        User user2 = new User("Bar", 2);
        User[] data = {user1, user2};
        FromElementsGeneratorFunction<User> generatorFunction =
                new FromElementsGeneratorFunction<>(data);
        DataGeneratorSource<User> dataGeneratorSource =
                new DataGeneratorSource<>(
                        generatorFunction, data.length, new AvroTypeInfo<>(User.class));
        DataStream<User> stream =
                env.fromSource(
                        dataGeneratorSource, WatermarkStrategy.noWatermarks(), "generator source");

        List<User> result = stream.executeAndCollect(data.length + 1);
        TypeSerializer<User> serializer = generatorFunction.getSerializer();

        assertThat(serializer).isInstanceOf(AvroSerializer.class);
        assertThat(result).containsExactly(data);
    }
}
