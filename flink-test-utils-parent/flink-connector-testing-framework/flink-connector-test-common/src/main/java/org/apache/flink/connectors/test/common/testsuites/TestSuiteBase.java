package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Base class for all test suites.
 *
 * <p>A well-designed test suite consists of:
 *
 * <ul>
 *   <li>Test cases, which are represented as methods in the test suite class.
 *   <li>Test Configurations, which is a collection of configuration required for running cases in
 *       this test suite.
 * </ul>
 *
 * <p>All cases should have well-descriptive JavaDoc, including:
 *
 * <ul>
 *   <li>What's the purpose of this case
 *   <li>Simple description of how this case works
 *   <li>Condition to fulfill in order to pass this case
 *   <li>Required test configurations of the case
 * </ul>
 */
public abstract class TestSuiteBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(TestSuiteBase.class);

    public abstract TestEnvironment getTestEnvironment();

    public abstract ExternalContext<T> getExternalContext();

    @After
    public void cleanup() throws Exception {
        getExternalContext().close();
    }

    // ----------------------------- Basic test cases ---------------------------------
    @Test(timeout = 60000L)
    public void testSourceSingleSplit() throws Exception {
        ExternalContext<T> externalContext = getExternalContext();
        TestEnvironment testEnv = getTestEnvironment();

        // Write test data to external system
        LOG.debug("Create 1 SourceSplit and write test data to it");
        SourceSplitDataWriter<T> splitWriter = externalContext.createSourceSplit();
        Collection<T> testRecords = externalContext.generateTestRecords();
        splitWriter.writeRecords(testRecords);
        LOG.debug("{} test records are written to split", testRecords.size());

        // Build and execute Flink job
        LOG.debug("Build and execute Flink job");
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();
        final CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                externalContext.createSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(1)
                        .executeAndCollect("Source Single Split Test");

        // Check test result
        LOG.debug("Check test result");
        checkSingleSplitTestResult(testRecords.iterator(), resultIterator);
        resultIterator.close();
        LOG.debug("Test passed");
    }

    @Test
    public void testMultipleSplits() throws Exception {
        ExternalContext<T> externalContext = getExternalContext();
        TestEnvironment testEnv = getTestEnvironment();

        // Create multiple splits and write test data to external system
        List<Collection<T>> testRecordCollections = new ArrayList<>();
        final int splitNum = 5;
        LOG.debug("Create {} splits and write test data to them", splitNum);
        for (int i = 0; i < splitNum; i++) {
            final SourceSplitDataWriter<T> splitWriter = externalContext.createSourceSplit();
            final Collection<T> testRecords = externalContext.generateTestRecords();
            splitWriter.writeRecords(testRecords);
            testRecordCollections.add(testRecords);
            LOG.debug("{} test records are written to split {}", testRecords.size(), i);
        }

        LOG.debug("Build and execute Flink job");
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();
        final CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                externalContext.createSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNum)
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

    @Test(timeout = 60000L)
    public void testRedundantParallelism() throws Exception {

        List<Collection<T>> testRecordsCollections = new ArrayList<>();

        int splitNum = 5;
        for (int i = 0; i < splitNum; i++) {
            final SourceSplitDataWriter<T> splitWriter = getExternalContext().createSourceSplit();
            final Collection<T> testRecords = getExternalContext().generateTestRecords();
            splitWriter.writeRecords(testRecords);
            testRecordsCollections.add(testRecords);
        }

        final CloseableIterator<T> resultIterator =
                getTestEnvironment()
                        .createExecutionEnvironment()
                        .fromSource(
                                getExternalContext().createSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNum + 1)
                        .executeAndCollect("Redundant Parallelism Test");

        checkMultipleSplitRecords(
                testRecordsCollections.stream()
                        .map(Collection::iterator)
                        .collect(Collectors.toList()),
                resultIterator);
    }

    // ----------------------------- Helper Functions ---------------------------------

    protected void checkEnvironmentIsControllable(TestEnvironment testEnvironment) {
        if (!ClusterControllable.class.isAssignableFrom(testEnvironment.getClass())) {
            throw new IllegalArgumentException("Provided test environment is not controllable.");
        }
    }

    protected void checkSingleSplitTestResult(
            Iterator<T> testRecordIterator, Iterator<T> resultRecordIterator, int limit) {
        for (int i = 0; i < limit; i++) {
            assertEquals(testRecordIterator.next(), resultRecordIterator.next());
            LOG.debug("Record No.{} checked", i);
        }
    }

    protected void checkSingleSplitTestResult(
            Iterator<T> testRecordIterator, Iterator<T> resultRecordIterator) {
        testRecordIterator.forEachRemaining(
                (record) -> assertEquals(record, resultRecordIterator.next()));

        assertFalse(resultRecordIterator.hasNext());
    }

    protected void checkMultipleSplitRecords(
            Collection<Iterator<T>> testRecordIterators, Iterator<T> resultRecordIterator) {

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
    }

    /**
     * Iterator with current.
     *
     * @param <E>
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

    protected static void validateRequiredConfigs(
            Configuration conf, ConfigOption<?>... requiredOptions) {
        for (ConfigOption<?> requiredOption : requiredOptions) {
            if (!conf.contains(requiredOption)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Missing '%s' in the provided test configurations. Required options are:\n\n%s",
                                requiredOption.key(),
                                Arrays.stream(requiredOptions)
                                        .map(ConfigOption::key)
                                        .collect(Collectors.joining("\n"))));
            }
        }
    }
}
