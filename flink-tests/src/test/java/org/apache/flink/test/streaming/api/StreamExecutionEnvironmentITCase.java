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

package org.apache.flink.test.streaming.api;

import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration tests for {@link StreamExecutionEnvironment}. */
public class StreamExecutionEnvironmentITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void executeThrowsProgramInvocationException() {
        Configuration config = new Configuration(MINI_CLUSTER.getClientConfiguration());
        config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME);
        config.set(DeploymentOptions.ATTACHED, true);

        // Create the execution environment explicitly from a Configuration so we know that we
        // don't get some other subclass. If we just did
        // StreamExecutionEnvironment.getExecutionEnvironment() we would get a
        // TestStreamEnvironment that the MiniClusterExtension created. We want to test the
        // behaviour
        // of the base environment, though.
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(config);

        env.fromData("hello")
                .map(
                        in -> {
                            throw new RuntimeException("Failing");
                        })
                .print();

        assertThatThrownBy(env::execute).isInstanceOf(ProgramInvocationException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAvroGenericRecordsInFromElementsDoesNotFailDueToKryoFallback() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Schema schema = getSchemaFromResources("/avro/user.avsc");
        GenericRecord user1 =
                new GenericRecordBuilder(schema).set("name", "Foo").set("age", 40).build();
        GenericRecord user2 =
                new GenericRecordBuilder(schema).set("name", "Bar").set("age", 45).build();
        GenericRecord[] data = {user1, user2};
        DataStream<GenericRecord> stream =
                env.fromData(new GenericRecordAvroTypeInfo(schema), data);

        List<GenericRecord> result = stream.executeAndCollect(data.length + 1);

        assertThat(result).containsExactly(data);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAvroGenericRecordsInFromElementsDoesNotFailDueToKryoFallbackUsingReturns()
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Schema schema = getSchemaFromResources("/avro/user.avsc");
        GenericRecord user1 =
                new GenericRecordBuilder(schema).set("name", "Foo").set("age", 40).build();
        GenericRecord user2 =
                new GenericRecordBuilder(schema).set("name", "Bar").set("age", 45).build();
        GenericRecord[] data = {user1, user2};
        DataStream<GenericRecord> stream =
                env.fromData(data).returns(new GenericRecordAvroTypeInfo(schema));

        List<GenericRecord> result = stream.executeAndCollect(data.length + 1);

        assertThat(result).containsExactly(data);
    }

    private Schema getSchemaFromResources(String path) throws Exception {
        try (InputStream schemaStream = getClass().getResourceAsStream(path)) {
            if (schemaStream == null) {
                throw new IllegalStateException("Could not find " + path + " in classpath");
            }
            return new Schema.Parser().parse(schemaStream);
        }
    }
}
