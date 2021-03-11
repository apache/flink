package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.connectors.test.common.utils.FlinkJobStatusHelper;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Base class for all test suites.
 *
 * <p>All cases should have well-descriptive JavaDoc, including:
 *
 * <ul>
 *   <li>What's the purpose of this case
 *   <li>Simple description of how this case works
 *   <li>Condition to fulfill in order to pass this case
 *   <li>Requirement of running this case
 * </ul>
 */
public abstract class TestSuiteBase<T> extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(TestSuiteBase.class);

    public abstract TestEnvironment getTestEnvironment();

    public abstract ExternalContext<T> getExternalContext();

    @After
    public void cleanup() throws Exception {
        getExternalContext().close();
    }

    // ----------------------------- Basic test cases ---------------------------------

    /**
     * Test connector source with only one split in the external system.
     *
     * <p>This test will create one split in the external system, write test data into it, and
     * consume back via a Flink job with 1 parallelism.
     *
     * <p>The number and order of records consumed by Flink need to be identical to the test data
     * written to the external system in order to pass this test.
     *
     * <p>A bounded source is required for this test.
     */
    @Test(timeout = 60000L)
    public void testSourceSingleSplit() throws Exception {

        // Write test data to external system
        final Collection<T> testRecords =
                generateAndWriteTestData(getExternalContext().createSourceSplit());

        // Build and execute Flink job
        LOG.debug("Submitting Flink job to test environment");
        StreamExecutionEnvironment execEnv = getTestEnvironment().createExecutionEnvironment();
        final CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                getExternalContext().createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(1)
                        .executeAndCollect("Source Single Split Test");

        // Check test result
        checkSingleSplitRecords(testRecords.iterator(), resultIterator);
        resultIterator.close();
    }

    /**
     * Test connector source with multiple splits in the external system
     *
     * <p>This test will create 4 splits in the external system, write test data to all splits, and
     * consume back via a Flink job with 5 parallelism.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @Test(timeout = 60000L)
    public void testMultipleSplits() throws Exception {

        final int splitNumber = 4;
        final List<Collection<T>> testRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordCollections.add(
                    generateAndWriteTestData(getExternalContext().createSourceSplit()));
        }

        LOG.debug("Build and execute Flink job");
        StreamExecutionEnvironment execEnv = getTestEnvironment().createExecutionEnvironment();
        final CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                getExternalContext().createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNumber)
                        .executeAndCollect("Source Multiple Split Test");

        // Check test result
        LOG.debug("Check test result");
        checkMultipleSplitRecords(
                testRecordCollections.stream()
                        .map(Collection::iterator)
                        .collect(Collectors.toList()),
                resultIterator);
        resultIterator.close();
        LOG.debug("Test passed");
    }

    /**
     * Test connector source with a redundant parallelism.
     *
     * <p>This test will create 4 split in the external system, write test data to all splits, and
     * consume back via a Flink job with 5 parallelism, so at least one parallelism / source reader
     * will be idle (assigned with no splits). If the split enumerator of the source doesn't signal
     * NoMoreSplitsEvent to the idle source reader, the Flink job will never spin to FINISHED state.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @Test(timeout = 60000L)
    public void testRedundantParallelism() throws Exception {

        final int splitNumber = 4;
        final List<Collection<T>> testRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordCollections.add(
                    generateAndWriteTestData(getExternalContext().createSourceSplit()));
        }

        final CloseableIterator<T> resultIterator =
                getTestEnvironment()
                        .createExecutionEnvironment()
                        .fromSource(
                                getExternalContext().createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNumber + 1)
                        .executeAndCollect("Redundant Parallelism Test");

        checkMultipleSplitRecords(
                testRecordCollections.stream()
                        .map(Collection::iterator)
                        .collect(Collectors.toList()),
                resultIterator);
    }

    /**
     * Test connector source with task manager failover.
     *
     * <p>This test will create 1 split in the external system, write test record set A into the
     * split, restart task manager to trigger job failover, write test record set B into the split,
     * and terminate the Flink job finally.
     *
     * <p>The number and order of records consumed by Flink should be identical to A before the
     * failover and B after the failover in order to pass the test.
     *
     * <p>An unbounded source is required for this test, since the test need to trigger task manager
     * failover in the middle of the test.
     */
    @Test(timeout = 300000)
    public void testTaskManagerFailure() throws Exception {

        checkEnvironmentIsControllable(getTestEnvironment());

        final Collection<T> testRecordsBeforeFailure = getExternalContext().generateTestRecords();
        final SourceSplitDataWriter<T> sourceSplitDataWriter =
                getExternalContext().createSourceSplit();
        sourceSplitDataWriter.writeRecords(testRecordsBeforeFailure);

        final StreamExecutionEnvironment env = getTestEnvironment().createExecutionEnvironment();

        env.enableCheckpointing(1000);
        final DataStreamSource<T> dataStreamSource =
                env.fromSource(
                                getExternalContext().createSource(Boundedness.CONTINUOUS_UNBOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(1);

        // Since DataStream API doesn't expose job client for executeAndCollect(), we have
        // to reuse these part of code to get both job client and result iterator :-(
        // ------------------------------------ START ---------------------------------------------
        TypeSerializer<T> serializer = dataStreamSource.getType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID().toString();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());
        CollectStreamSink<T> sink = new CollectStreamSink<>(dataStreamSource, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        final JobClient jobClient = env.executeAsync("TaskManager Failover Test");
        iterator.setJobClient(jobClient);
        // -------------------------------------- END ---------------------------------------------

        checkSingleSplitRecords(
                testRecordsBeforeFailure.iterator(), iterator, testRecordsBeforeFailure.size());

        // -------------------------------- Trigger failover ---------------------------------------
        final ClusterControllable controller = (ClusterControllable) getTestEnvironment();
        controller.triggerTaskManagerFailover(jobClient, () -> {});

        FlinkJobStatusHelper.waitForJobStatus(
                jobClient, Collections.singletonList(JobStatus.RUNNING), Duration.ofSeconds(30));

        final Collection<T> testRecordsAfterFailure = getExternalContext().generateTestRecords();
        sourceSplitDataWriter.writeRecords(testRecordsAfterFailure);
        checkSingleSplitRecords(
                testRecordsAfterFailure.iterator(), iterator, testRecordsAfterFailure.size());

        iterator.close();
        FlinkJobStatusHelper.terminateJob(jobClient, Duration.ofSeconds(30));
        FlinkJobStatusHelper.waitForJobStatus(
                jobClient, Collections.singletonList(JobStatus.CANCELED), Duration.ofSeconds(30));
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param splitWriter Split writer to write test records into
     * @return Collection of generated test records
     */
    protected Collection<T> generateAndWriteTestData(SourceSplitDataWriter<T> splitWriter) {
        final Collection<T> testRecordCollection = getExternalContext().generateTestRecords();
        LOG.debug("Writing {} records to external system", testRecordCollection.size());
        splitWriter.writeRecords(testRecordCollection);
        return testRecordCollection;
    }

    /**
     * Check if the given test environment is controllable (can trigger failover or network
     * isolation).
     *
     * @param testEnvironment Test environment being checked
     * @throws IllegalArgumentException if the test environment is not controllable
     */
    protected void checkEnvironmentIsControllable(TestEnvironment testEnvironment)
            throws IllegalArgumentException {
        if (!ClusterControllable.class.isAssignableFrom(testEnvironment.getClass())) {
            throw new IllegalArgumentException("Provided test environment is not controllable.");
        }
    }

    /**
     * Check if records consumed by Flink are identical to test records written into the single
     * split within the limit of record number.
     *
     * @param testRecordIterator Iterator of test records
     * @param resultRecordIterator Iterator of result records consumed by Flink
     * @param limit Number of records to check
     */
    protected void checkSingleSplitRecords(
            Iterator<T> testRecordIterator, Iterator<T> resultRecordIterator, int limit) {
        for (int i = 0; i < limit; i++) {
            assertEquals(testRecordIterator.next(), resultRecordIterator.next());
        }
        LOG.debug("{} records are validated", limit);
    }

    /**
     * Check if records consumed by Flink are identical to test records written into the single
     * split.
     *
     * @param testRecordIterator Iterator of test records
     * @param resultRecordIterator Iterator of result records consumed by Flink
     */
    protected void checkSingleSplitRecords(
            Iterator<T> testRecordIterator, Iterator<T> resultRecordIterator) {
        int recordCounter = 0;

        while (testRecordIterator.hasNext()) {
            assertEquals(testRecordIterator.next(), resultRecordIterator.next());
            recordCounter++;
        }

        assertFalse(resultRecordIterator.hasNext());
        LOG.debug("{} records are validated", recordCounter);
    }

    /**
     * Check if records consumed by Flink is identical to test records written into multiple splits.
     *
     * <p>The order of records across different splits can be arbitrary, but should be identical
     * within a split.
     *
     * @param testRecordIterators Collection of iterators for records in different splits
     * @param resultRecordIterator Iterator of result records consumed by Flink
     */
    protected void checkMultipleSplitRecords(
            Collection<Iterator<T>> testRecordIterators, Iterator<T> resultRecordIterator) {
        int recordCounter = 0;

        final List<IteratorWithCurrent<T>> testRecordIteratorsWrapped =
                testRecordIterators.stream()
                        .map(
                                (Function<Iterator<T>, IteratorWithCurrent<T>>)
                                        IteratorWithCurrent::new)
                        .collect(Collectors.toList());

        while (resultRecordIterator.hasNext()) {
            T currentRecord = resultRecordIterator.next();
            for (IteratorWithCurrent<T> testRecordIterator : testRecordIteratorsWrapped) {
                if (currentRecord.equals(testRecordIterator.current())) {
                    testRecordIterator.next();
                    recordCounter++;
                }
            }
        }

        testRecordIteratorsWrapped.forEach(
                iterator ->
                        assertFalse(
                                "One of test record iterators does not reach the end. "
                                        + "This indicated that records received is less than records "
                                        + "sent to the external system. ",
                                iterator.hasNext()));

        LOG.debug("{} records are validated", recordCounter);
    }

    /**
     * An iterator wrapper which can access the element that the iterator is currently pointing to.
     *
     * @param <E> The type of elements returned by this iterator.
     */
    public static class IteratorWithCurrent<E> implements Iterator<E> {

        private final Iterator<E> originalIterator;
        private E current;

        public IteratorWithCurrent(Iterator<E> originalIterator) {
            this.originalIterator = originalIterator;
            try {
                current = originalIterator.next();
            } catch (NoSuchElementException e) {
                current = null;
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public E next() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            E previous = current;
            if (originalIterator.hasNext()) {
                current = originalIterator.next();
            } else {
                current = null;
            }
            return previous;
        }

        public E current() {
            return current;
        }
    }
}
