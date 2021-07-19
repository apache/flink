package org.apache.flink.glue.schema.registry.test.json.specific;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.glue.schema.registry.test.json.GSRKinesisPubsubClient;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Test driver for {@link GlueSchemaRegistryPojoKinesisExample#main}. */
public class GlueSchemaRegistryPojoKinesisExampleTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(GlueSchemaRegistryPojoKinesisExampleTest.class);

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
                                GlueSchemaRegistryPojoKinesisExample.main(args);
                                // this message won't appear in the log,
                                // job is terminated when shutting down cluster
                                LOG.info("executed program");
                            } catch (Exception e) {
                                executeException.set(e);
                            }
                        });
        executeThread.start();

        List<Car> messages = getRecords();
        for (Car msg : messages) {
            String schema =
                    "{\"$schema\":\"http://json-schema.org/draft-04/schema#\","
                            + "\"title\":\"Simple Car Schema\","
                            + "\"type\":\"object\","
                            + "\"additionalProperties\":false,"
                            + "\"description\":\"This is a car\","
                            + "\"className\":\"org.apache.flink.glue.schema.registry.test.json.specific.Car\","
                            + "\"properties\":{"
                            + "\"make\":{\"type\":\"string\"},"
                            + "\"model\":{\"type\":\"string\"},"
                            + "\"used\":{\"type\":\"boolean\",\"default\":true},"
                            + "\"miles\":{\"type\":\"integer\",\"maximum\":200000,\"multipleOf\":1000},"
                            + "\"year\":{\"type\":\"integer\",\"minimum\":2000},"
                            + "\"purchaseDate\":{\"type\":\"integer\",\"format\":\"utc-millisec\"},"
                            + "\"listedDate\":{\"type\":\"integer\",\"format\":\"utc-millisec\"},"
                            + "\"owners\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
                            + "\"serviceChecks\":{\"type\":\"array\",\"items\":{\"type\":\"number\"}}},"
                            + "\"required\":[\"make\",\"model\",\"used\",\"miles\",\"year\"]}";
            pubsub.sendMessage(schema, inputStream, msg);
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

        List<Car> expectedResults = getRecords();
        LOG.info("expected results: {}", expectedResults);

        for (Object expectedResult : expectedResults) {
            Assert.assertTrue(results.contains(expectedResult));
        }

        LOG.info("test finished");
        System.exit(0);
    }

    private static List<Car> getRecords() {
        List<Car> records = new ArrayList<>();
        Car honda =
                Car.builder()
                        .make("Honda")
                        .model("crv")
                        .used(true)
                        .miles(10000)
                        .year(2002)
                        .listedDate(new GregorianCalendar(2002, Calendar.FEBRUARY, 11).getTime())
                        .purchaseDate(Date.from(Instant.parse("2005-01-01T00:00:00.000Z")))
                        .owners(new String[] {"John", "Jane", "Hu"})
                        .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                        .build();

        Car tesla =
                Car.builder()
                        .make("Tesla")
                        .model("3")
                        .used(false)
                        .miles(20000)
                        .year(2020)
                        .listedDate(new GregorianCalendar(2020, Calendar.FEBRUARY, 20).getTime())
                        .purchaseDate(Date.from(Instant.parse("2021-01-01T00:00:00.000Z")))
                        .owners(new String[] {"Harry", "Megan"})
                        .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                        .build();

        Car bmw =
                Car.builder()
                        .make("BMW")
                        .model("550")
                        .used(false)
                        .miles(5000)
                        .year(2018)
                        .listedDate(new GregorianCalendar(2018, Calendar.JULY, 6).getTime())
                        .purchaseDate(Date.from(Instant.parse("2019-01-01T00:00:00.000Z")))
                        .owners(new String[] {"Austin", "Leo"})
                        .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                        .build();

        records.add(honda);
        records.add(tesla);
        records.add(bmw);
        return records;
    }
}
