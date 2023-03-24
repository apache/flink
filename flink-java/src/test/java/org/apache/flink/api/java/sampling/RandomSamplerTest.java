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

package org.apache.flink.api.java.sampling;

import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.extensions.retry.RetryExtension;
import org.apache.flink.util.Preconditions;

import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test suite try to verify whether all the random samplers work as we expected, which mainly
 * focus on:
 *
 * <ul>
 *   <li>Does sampled result fit into input parameters? we check parameters like sample fraction,
 *       sample size, w/o replacement, and so on.
 *   <li>Does sampled result randomly selected? we verify this by measure how much does it
 *       distributed on source data. Run Kolmogorov-Smirnov (KS) test between the random samplers
 *       and default reference samplers which is distributed well-proportioned on source data. If
 *       random sampler select elements randomly from source, it would distributed well-proportioned
 *       on source data as well. The KS test will fail to strongly reject the null hypothesis that
 *       the distributions of sampling gaps are the same.
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test">Kolmogorov Smirnov
 *     test</a>
 */
@ExtendWith(RetryExtension.class)
class RandomSamplerTest {

    private static final int SOURCE_SIZE = 10000;

    private static final int DEFAULT_PARTITION_NUMBER = 10;

    private static final KolmogorovSmirnovTest ksTest = new KolmogorovSmirnovTest();

    private static final List<Double> source = new ArrayList<>(SOURCE_SIZE);

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final List<Double>[] sourcePartitions = new List[DEFAULT_PARTITION_NUMBER];

    @BeforeAll
    static void init() {
        // initiate source data set.
        for (int i = 0; i < SOURCE_SIZE; i++) {
            source.add((double) i);
        }
    }

    private void initSourcePartition() {
        for (int i = 0; i < DEFAULT_PARTITION_NUMBER; i++) {
            sourcePartitions[i] =
                    new ArrayList<>(
                            (int) Math.ceil((double) SOURCE_SIZE / DEFAULT_PARTITION_NUMBER));
        }
        for (int i = 0; i < SOURCE_SIZE; i++) {
            int index = i % DEFAULT_PARTITION_NUMBER;
            sourcePartitions[index].add((double) i);
        }
    }

    @Test
    void testBernoulliSamplerWithUnexpectedFraction1() {
        assertThatThrownBy(() -> verifySamplerFraction(-1, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBernoulliSamplerWithUnexpectedFraction2() {
        assertThatThrownBy(() -> verifySamplerFraction(2, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testBernoulliSamplerFraction() {
        verifySamplerFraction(0.01, false);
        verifySamplerFraction(0.05, false);
        verifySamplerFraction(0.1, false);
        verifySamplerFraction(0.3, false);
        verifySamplerFraction(0.5, false);
        verifySamplerFraction(0.854, false);
        verifySamplerFraction(0.99, false);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testBernoulliSamplerDuplicateElements() {
        verifyRandomSamplerDuplicateElements(new BernoulliSampler<>(0.01));
        verifyRandomSamplerDuplicateElements(new BernoulliSampler<>(0.1));
        verifyRandomSamplerDuplicateElements(new BernoulliSampler<>(0.5));
    }

    @Test
    void testPoissonSamplerWithUnexpectedFraction1() {
        assertThatThrownBy(() -> verifySamplerFraction(-1, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testPoissonSamplerFraction() {
        verifySamplerFraction(0.01, true);
        verifySamplerFraction(0.05, true);
        verifySamplerFraction(0.1, true);
        verifySamplerFraction(0.5, true);
        verifySamplerFraction(0.854, true);
        verifySamplerFraction(0.99, true);
        verifySamplerFraction(1.5, true);
    }

    @Test
    void testReservoirSamplerUnexpectedSize1() {
        assertThatThrownBy(() -> verifySamplerFixedSampleSize(-1, true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReservoirSamplerUnexpectedSize2() {
        assertThatThrownBy(() -> verifySamplerFixedSampleSize(-1, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testBernoulliSamplerDistribution() {
        verifyBernoulliSampler(0.01d);
        verifyBernoulliSampler(0.05d);
        verifyBernoulliSampler(0.1d);
        verifyBernoulliSampler(0.5d);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testPoissonSamplerDistribution() {
        verifyPoissonSampler(0.01d);
        verifyPoissonSampler(0.05d);
        verifyPoissonSampler(0.1d);
        verifyPoissonSampler(0.5d);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerSampledSize() {
        verifySamplerFixedSampleSize(1, true);
        verifySamplerFixedSampleSize(10, true);
        verifySamplerFixedSampleSize(100, true);
        verifySamplerFixedSampleSize(1234, true);
        verifySamplerFixedSampleSize(9999, true);
        verifySamplerFixedSampleSize(20000, true);

        verifySamplerFixedSampleSize(1, false);
        verifySamplerFixedSampleSize(10, false);
        verifySamplerFixedSampleSize(100, false);
        verifySamplerFixedSampleSize(1234, false);
        verifySamplerFixedSampleSize(9999, false);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerSampledSize2() {
        RandomSampler<Double> sampler = new ReservoirSamplerWithoutReplacement<>(20000);
        Iterator<Double> sampled = sampler.sample(source.iterator());
        assertThat(getSize(sampled))
                .as(
                        "ReservoirSamplerWithoutReplacement sampled output size should not beyond the source size.")
                .isEqualTo(SOURCE_SIZE);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerDuplicateElements() {
        verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<>(100));
        verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<>(1000));
        verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<>(5000));
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerWithoutReplacement() {
        verifyReservoirSamplerWithoutReplacement(100, false);
        verifyReservoirSamplerWithoutReplacement(500, false);
        verifyReservoirSamplerWithoutReplacement(1000, false);
        verifyReservoirSamplerWithoutReplacement(5000, false);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerWithReplacement() {
        verifyReservoirSamplerWithReplacement(100, false);
        verifyReservoirSamplerWithReplacement(500, false);
        verifyReservoirSamplerWithReplacement(1000, false);
        verifyReservoirSamplerWithReplacement(5000, false);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerWithMultiSourcePartitions1() {
        initSourcePartition();

        verifyReservoirSamplerWithoutReplacement(100, true);
        verifyReservoirSamplerWithoutReplacement(500, true);
        verifyReservoirSamplerWithoutReplacement(1000, true);
        verifyReservoirSamplerWithoutReplacement(5000, true);
    }

    @TestTemplate
    @RetryOnFailure(times = 3)
    void testReservoirSamplerWithMultiSourcePartitions2() {
        initSourcePartition();

        verifyReservoirSamplerWithReplacement(100, true);
        verifyReservoirSamplerWithReplacement(500, true);
        verifyReservoirSamplerWithReplacement(1000, true);
        verifyReservoirSamplerWithReplacement(5000, true);
    }

    /*
     * Sample with fixed size, verify whether the sampled result size equals to input size.
     */
    private void verifySamplerFixedSampleSize(int numSample, boolean withReplacement) {
        RandomSampler<Double> sampler;
        if (withReplacement) {
            sampler = new ReservoirSamplerWithReplacement<>(numSample);
        } else {
            sampler = new ReservoirSamplerWithoutReplacement<>(numSample);
        }
        Iterator<Double> sampled = sampler.sample(source.iterator());
        assertThat(getSize(sampled)).isEqualTo(numSample);
    }

    /*
     * Sample with fraction, and verify whether the sampled result close to input fraction.
     */
    private void verifySamplerFraction(double fraction, boolean withReplacement) {
        RandomSampler<Double> sampler;
        if (withReplacement) {
            sampler = new PoissonSampler<>(fraction);
        } else {
            sampler = new BernoulliSampler<>(fraction);
        }

        // take 20 times sample, and take the average result size for next step comparison.
        int totalSampledSize = 0;
        double sampleCount = 20;
        for (int i = 0; i < sampleCount; i++) {
            totalSampledSize += getSize(sampler.sample(source.iterator()));
        }
        double resultFraction = totalSampledSize / ((double) SOURCE_SIZE * sampleCount);
        assertThat(Math.abs((resultFraction - fraction) / fraction))
                .as(
                        String.format(
                                "expected fraction: %f, result fraction: %f",
                                fraction, resultFraction))
                .isLessThan(0.2);
    }

    /*
     * Test sampler without replacement, and verify that there should not exist any duplicate element in sampled result.
     */
    private void verifyRandomSamplerDuplicateElements(final RandomSampler<Double> sampler) {
        Iterator<Double> values = sampler.sample(source.iterator());
        Set<Double> set = new HashSet<>();
        while (values.hasNext()) {
            double next = values.next();
            assertThat(set.add(next))
                    .as("Sampler returned duplicate element (" + next + "). Set=" + set)
                    .isTrue();
        }
    }

    private int getSize(Iterator<?> iterator) {
        int size = 0;
        while (iterator.hasNext()) {
            iterator.next();
            size++;
        }
        return size;
    }

    private void verifyBernoulliSampler(double fraction) {
        BernoulliSampler<Double> sampler = new BernoulliSampler<>(fraction);
        verifyRandomSamplerWithFraction(fraction, sampler, true);
        verifyRandomSamplerWithFraction(fraction, sampler, false);
    }

    private void verifyPoissonSampler(double fraction) {
        PoissonSampler<Double> sampler = new PoissonSampler<>(fraction);
        verifyRandomSamplerWithFraction(fraction, sampler, true);
        verifyRandomSamplerWithFraction(fraction, sampler, false);
    }

    private void verifyReservoirSamplerWithReplacement(
            int numSamplers, boolean sampleOnPartitions) {
        ReservoirSamplerWithReplacement<Double> sampler =
                new ReservoirSamplerWithReplacement<>(numSamplers);
        verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions);
        verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions);
    }

    private void verifyReservoirSamplerWithoutReplacement(
            int numSamplers, boolean sampleOnPartitions) {
        ReservoirSamplerWithoutReplacement<Double> sampler =
                new ReservoirSamplerWithoutReplacement<>(numSamplers);
        verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions);
        verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions);
    }

    /*
     * Verify whether random sampler sample with fraction from source data randomly. There are two default sample, one is
     * sampled from source data with certain interval, the other is sampled only from the first half region of source data,
     * If random sampler select elements randomly from source, it would distributed well-proportioned on source data as well,
     * so the K-S Test result would accept the first one, while reject the second one.
     */
    private void verifyRandomSamplerWithFraction(
            double fraction, RandomSampler<Double> sampler, boolean withDefaultSampler) {
        double[] baseSample;
        if (withDefaultSampler) {
            baseSample = getDefaultSampler(fraction);
        } else {
            baseSample = getWrongSampler(fraction);
        }

        verifyKSTest(sampler, baseSample, withDefaultSampler);
    }

    /*
     * Verify whether random sampler sample with fixed size from source data randomly. There are two default sample, one is
     * sampled from source data with certain interval, the other is sampled only from the first half region of source data,
     * If random sampler select elements randomly from source, it would distributed well-proportioned on source data as well,
     * so the K-S Test result would accept the first one, while reject the second one.
     */
    private void verifyRandomSamplerWithSampleSize(
            int sampleSize,
            RandomSampler<Double> sampler,
            boolean withDefaultSampler,
            boolean sampleWithPartitions) {
        double[] baseSample;
        if (withDefaultSampler) {
            baseSample = getDefaultSampler(sampleSize);
        } else {
            baseSample = getWrongSampler(sampleSize);
        }

        verifyKSTest(sampler, baseSample, withDefaultSampler, sampleWithPartitions);
    }

    private void verifyKSTest(
            RandomSampler<Double> sampler, double[] defaultSampler, boolean expectSuccess) {
        verifyKSTest(sampler, defaultSampler, expectSuccess, false);
    }

    private void verifyKSTest(
            RandomSampler<Double> sampler,
            double[] defaultSampler,
            boolean expectSuccess,
            boolean sampleOnPartitions) {
        double[] sampled = getSampledOutput(sampler, sampleOnPartitions);
        double pValue = ksTest.kolmogorovSmirnovStatistic(sampled, defaultSampler);
        double dValue = getDValue(sampled.length, defaultSampler.length);
        if (expectSuccess) {
            assertThat(pValue)
                    .as(
                            String.format(
                                    "KS test result with p value(%f), d value(%f)", pValue, dValue))
                    .isLessThanOrEqualTo(dValue);
        } else {
            assertThat(pValue)
                    .as(
                            String.format(
                                    "KS test result with p value(%f), d value(%f)", pValue, dValue))
                    .isGreaterThan(dValue);
        }
    }

    private double[] getSampledOutput(RandomSampler<Double> sampler, boolean sampleOnPartitions) {
        Iterator<Double> sampled;
        if (sampleOnPartitions) {
            DistributedRandomSampler<Double> reservoirRandomSampler =
                    (DistributedRandomSampler<Double>) sampler;
            List<IntermediateSampleData<Double>> intermediateResult = new LinkedList<>();
            for (int i = 0; i < DEFAULT_PARTITION_NUMBER; i++) {
                Iterator<IntermediateSampleData<Double>> partialIntermediateResult =
                        reservoirRandomSampler.sampleInPartition(sourcePartitions[i].iterator());
                while (partialIntermediateResult.hasNext()) {
                    intermediateResult.add(partialIntermediateResult.next());
                }
            }
            sampled = reservoirRandomSampler.sampleInCoordinator(intermediateResult.iterator());
        } else {
            sampled = sampler.sample(source.iterator());
        }
        List<Double> list = new ArrayList<>();
        while (sampled.hasNext()) {
            list.add(sampled.next());
        }
        return transferFromListToArrayWithOrder(list);
    }

    /*
     * Some sample result may not order by the input sequence, we should make it in order to do K-S test.
     */
    private double[] transferFromListToArrayWithOrder(List<Double> list) {
        Collections.sort(list);
        double[] result = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    private double[] getDefaultSampler(double fraction) {
        Preconditions.checkArgument(fraction > 0, "Sample fraction should be positive.");
        int size = (int) (SOURCE_SIZE * fraction);
        double step = 1 / fraction;
        double[] defaultSampler = new double[size];
        for (int i = 0; i < size; i++) {
            defaultSampler[i] = Math.round(step * i);
        }

        return defaultSampler;
    }

    private double[] getDefaultSampler(int fixSize) {
        Preconditions.checkArgument(fixSize > 0, "Sample fraction should be positive.");
        double step = SOURCE_SIZE / (double) fixSize;
        double[] defaultSampler = new double[fixSize];
        for (int i = 0; i < fixSize; i++) {
            defaultSampler[i] = Math.round(step * i);
        }

        return defaultSampler;
    }

    /*
     * Build a failed sample distribution which only contains elements in the first half of source data.
     */
    private double[] getWrongSampler(double fraction) {
        Preconditions.checkArgument(fraction > 0, "Sample size should be positive.");
        int size = (int) (SOURCE_SIZE * fraction);
        int halfSourceSize = SOURCE_SIZE / 2;
        double[] wrongSampler = new double[size];
        for (int i = 0; i < size; i++) {
            wrongSampler[i] = (double) i % halfSourceSize;
        }

        return wrongSampler;
    }

    /*
     * Build a failed sample distribution which only contains elements in the first half of source data.
     */
    private double[] getWrongSampler(int fixSize) {
        Preconditions.checkArgument(fixSize > 0, "Sample size be positive.");
        int halfSourceSize = SOURCE_SIZE / 2;
        double[] wrongSampler = new double[fixSize];
        for (int i = 0; i < fixSize; i++) {
            wrongSampler[i] = (double) i % halfSourceSize;
        }

        return wrongSampler;
    }

    /*
     * Calculate the D value of K-S test for p-value 0.001, m and n are the sample size
     */
    private double getDValue(int m, int n) {
        Preconditions.checkArgument(m > 0, "input sample size should be positive.");
        Preconditions.checkArgument(n > 0, "input sample size should be positive.");
        return 1.95 * Math.sqrt(((double) m + (double) n) / ((double) m * (double) n));
    }
}
