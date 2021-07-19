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

package org.apache.flink.glue.schema.registry.test.json.generic;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.glue.schema.registry.test.json.GSRKinesisPubsubClient;

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Test driver for {@link GlueSchemaRegistryGenericKinesisExample#main}. */
public class GlueSchemaRegistryGenericKinesisExampleTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistryGenericKinesisExampleTest.class);

    public static void main(String[] args) throws Exception {
        LOG.info("System properties: {}", System.getProperties());
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String inputStream = parameterTool.getRequired("input-stream");
        String outputStream = parameterTool.getRequired("output-stream");

        GSRKinesisPubsubClient pubsub = new GSRKinesisPubsubClient(parameterTool.getProperties());
        pubsub.createStream(inputStream, 2, parameterTool.getProperties());
        pubsub.createStream(outputStream, 2, parameterTool.getProperties());

        // The example job needs to start after streams are created and run in parallel to the
        // validation logic.
        // The thread that runs the job won't terminate, we don't have a job reference to cancel it.
        // Once results are validated, the driver main thread will exit; job/cluster will be
        // terminated from script.
        final AtomicReference<Exception> executeException = new AtomicReference<>();
        Thread executeThread =
                new Thread(
                        () -> {
                            try {
                                GlueSchemaRegistryGenericKinesisExample.main(args);
                                // this message won't appear in the log,
                                // job is terminated when shutting down cluster
                                LOG.info("executed program");
                            } catch (Exception e) {
                                executeException.set(e);
                            }
                        });
        executeThread.start();

        List<JsonDataWithSchema> messages = getRecords();
        for (JsonDataWithSchema msg : messages) {
            pubsub.sendMessage(msg.getSchema(), inputStream, msg);
        }
        LOG.info("generated records");

        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(60));
        List<Object> results = pubsub.readAllMessages(outputStream);
        while (deadline.hasTimeLeft()
                && executeException.get() == null
                && results.size() < messages.size()) {
            LOG.info("waiting for results..");
            Thread.sleep(1000);
            results = pubsub.readAllMessages(outputStream);
        }

        if (executeException.get() != null) {
            throw executeException.get();
        }

        LOG.info("results: {}", results);
        Assert.assertEquals(
                "Results received from '" + outputStream + "': " + results,
                messages.size(),
                results.size());

        List<JsonDataWithSchema> expectedResults = getRecords();
        LOG.info("expected results: {}", expectedResults);

        for (Object expectedResult : expectedResults) {
            Assert.assertTrue(results.contains(expectedResult));
        }

        LOG.info("test finished");
        System.exit(0);
    }

    private static List<JsonDataWithSchema> getRecords() {
        List<JsonDataWithSchema> records = new ArrayList<>();
        String schema =
                "{\"$id\":\"https://example.com/address.schema.json\","
                        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
                        + "\"type\":\"object\","
                        + "\"properties\":{"
                        + "\"f1\":{\"type\":\"string\"},"
                        + "\"f2\":{\"type\":\"integer\",\"maximum\":1000}}}";
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"covid\",\"f2\":19}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"iphone\",\"f2\":12}").build());
        records.add(
                JsonDataWithSchema.builder(schema, "{\"f1\":\"Stranger Things\",\"f2\":4}")
                        .build());
        records.add(
                JsonDataWithSchema.builder(
                                schema, "{\"f1\":\"Friends\",\"f2\":6,\"f3\":\"coming soon\"}")
                        .build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"Porsche\",\"f2\":911}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"Ferrari\",\"f2\":488}").build());
        records.add(JsonDataWithSchema.builder(schema, "{\"f1\":\"McLaren\",\"f2\":720}").build());
        records.add(
                JsonDataWithSchema.builder(
                                schema, "{\"f1\":\"Panorama\",\"f2\":360,\"f3\":\"Fantastic!\"}")
                        .build());
        return records;
    }
}
