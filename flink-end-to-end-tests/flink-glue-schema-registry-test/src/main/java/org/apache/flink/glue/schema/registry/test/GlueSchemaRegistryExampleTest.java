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

package org.apache.flink.glue.schema.registry.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Test driver for {@link GlueSchemaRegistryExample#main}. */
public class GlueSchemaRegistryExampleTest {
    private static final Logger LOG = LoggerFactory.getLogger(GlueSchemaRegistryExampleTest.class);

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
                                GlueSchemaRegistryExample.main(args);
                                // this message won't appear in the log,
                                // job is terminated when shutting down cluster
                                LOG.info("executed program");
                            } catch (Exception e) {
                                executeException.set(e);
                            }
                        });
        executeThread.start();

        List<GenericRecord> messages = getRecords();
        for (GenericRecord msg : messages) {
            pubsub.sendMessage(GlueSchemaRegistryExample.getSchema().toString(), inputStream, msg);
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

        List<GenericRecord> expectedResults = getRecords();

        for (Object expectedResult : expectedResults) {
            Assert.assertTrue(results.contains(expectedResult));
        }

        LOG.info("test finished");
        System.exit(0);
    }

    private static List<GenericRecord> getRecords() throws IOException {
        Schema userSchema = GlueSchemaRegistryExample.getSchema();

        GenericRecord sansa = new GenericData.Record(userSchema);
        sansa.put("name", "Sansa");
        sansa.put("favorite_number", 99);
        sansa.put("favorite_color", "white");

        GenericRecord harry = new GenericData.Record(userSchema);
        harry.put("name", "Harry");
        harry.put("favorite_number", 10);
        harry.put("favorite_color", "black");

        GenericRecord hermione = new GenericData.Record(userSchema);
        hermione.put("name", "Hermione");
        hermione.put("favorite_number", 1);
        hermione.put("favorite_color", "red");

        GenericRecord ron = new GenericData.Record(userSchema);
        ron.put("name", "Ron");
        ron.put("favorite_number", 18);
        ron.put("favorite_color", "green");

        GenericRecord jay = new GenericData.Record(userSchema);
        jay.put("name", "Jay");
        jay.put("favorite_number", 0);
        jay.put("favorite_color", "blue");

        List<GenericRecord> records = new ArrayList<>();
        records.add(sansa);
        records.add(harry);
        records.add(hermione);
        records.add(ron);
        records.add(jay);
        return records;
    }
}
