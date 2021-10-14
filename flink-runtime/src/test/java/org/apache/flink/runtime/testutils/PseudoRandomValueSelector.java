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

package org.apache.flink.runtime.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.TestNameProvider;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

/**
 * Initializes the {@link Configuration} for particular {@link ConfigOption}s with random values if
 * unset.
 *
 * <p>With the same seed, the same values are always selected if the {@link #select(Configuration,
 * ConfigOption, Object[])} invocation happens in the same order. A different seed should select
 * different values.
 *
 * <p>The seed is calculated from a global seed (~unique per build) and a seed specific to test
 * cases. Thus, two different builds will mostly result in different values for the same test case.
 * Similarly, two test cases in the same build will have different randomized values.
 *
 * <p>The seed can be set with the maven/system property test.randomization.seed and is set by
 * default to commit id. If the seed is empty, {@link EnvironmentInformation} and as a last fallback
 * git command is used to retrieve the commit id.
 */
@Internal
@NotThreadSafe
public class PseudoRandomValueSelector {
    private static final Logger LOG = LoggerFactory.getLogger(PseudoRandomValueSelector.class);

    private final Function<Integer, Integer> randomValueSupplier;

    private static final long GLOBAL_SEED = (long) getGlobalSeed().hashCode() << 32;

    private PseudoRandomValueSelector(Function<Integer, Integer> randomValueSupplier) {
        this.randomValueSupplier = randomValueSupplier;
    }

    public <T> void select(Configuration configuration, ConfigOption<T> option, T... alternatives) {
        if (configuration.contains(option)) {
            return;
        }
        final int choice = randomValueSupplier.apply(alternatives.length);
        T value = alternatives[choice];
        LOG.info("Randomly selected {} for {}", value, option.key());
        configuration.set(option, value);
    }

    public static PseudoRandomValueSelector create(Object entryPointSeed) {
        final long combinedSeed = GLOBAL_SEED | entryPointSeed.hashCode();
        final Random random = new Random(combinedSeed);
        return new PseudoRandomValueSelector(random::nextInt);
    }

    private static String getGlobalSeed() {
        // manual seed or set by maven
        final String seed = System.getProperty("test.randomization.seed");
        if (seed != null && !seed.isEmpty()) {
            return seed;
        }

        // Read with git command (if installed)
        final Optional<String> gitCommitId = getGitCommitId();
        if (gitCommitId.isPresent()) {
            return gitCommitId.get();
        }

        // try EnvironmentInformation, which is set in the maven process
        final String commitId = EnvironmentInformation.getGitCommitId();
        if (!commitId.equals(EnvironmentInformation.UNKNOWN_COMMIT_ID)) {
            return commitId;
        }

        LOG.warn(
                "Test randomization was enabled but neither test.randomization.seed was configured nor could the commit hash be retrieved from git or the EnvironmentInformation. Please set the test.randomization.seed property manually to make the build reproducible.");
        // return any constant
        return "";
    }

    @VisibleForTesting
    public static Optional<String> getGitCommitId() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD").start();
            try (InputStream input = process.getInputStream()) {
                final String commit = IOUtils.toString(input, Charset.defaultCharset()).trim();
                if (commit.matches("[a-f0-9]{40}")) {
                    return Optional.of(commit);
                }
                LOG.debug("Cannot parse {}", commit);
            }
        } catch (IOException e) {
            LOG.debug("Could not invoke git", e);
        }
        return Optional.empty();
    }

    public static <T> void randomize(Configuration conf, ConfigOption<T> option, T... t1) {
        final String testName = TestNameProvider.getCurrentTestName();
        final PseudoRandomValueSelector valueSelector =
                PseudoRandomValueSelector.create(testName != null ? testName : "unknown");
        valueSelector.select(conf, option, t1);
    }
}
