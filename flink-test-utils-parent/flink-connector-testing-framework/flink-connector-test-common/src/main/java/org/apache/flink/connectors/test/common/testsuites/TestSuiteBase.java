package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.stream.Collectors;

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
public class TestSuiteBase {
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
