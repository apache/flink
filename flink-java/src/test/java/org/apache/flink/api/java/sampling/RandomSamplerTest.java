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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test suite try to verify whether all the random samplers work as we expected, which mainly focus on:
 * <ul>
 * <li>Does sampled result fit into input parameters? we check parameters like sample fraction, sample size,
 * w/o replacement, and so on.</li>
 * <li>Does sampled result randomly selected? we verify this by measure how much does it distributed on source data.
 * Run Kolmogorov-Smirnov (KS) test between the random samplers and default reference samplers which is distributed
 * well-proportioned on source data. If random sampler select elements randomly from source, it would distributed
 * well-proportioned on source data as well. The KS test will fail to strongly reject the null hypothesis that
 * the distributions of sampling gaps are the same.
 * </li>
 * <li>Does weighted sampler selected correctly? We count every item in the reservoir to get the probability 
 * after sampling and comparing with expected probabilities to verify its correctness.</li>
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test">Kolmogorov Smirnov test</a>
 */
public class RandomSamplerTest {
	private final static int SOURCE_SIZE = 10000;
	private static KolmogorovSmirnovTest ksTest;
	private static List<Double> source;
	private static List<WeightedData<Double>> source2;
	private final static int DEFFAULT_PARTITION_NUMBER = 10;
	private List<Double>[] sourcePartitions = new List[DEFFAULT_PARTITION_NUMBER];
	private List<WeightedData<Double>>[] sourcePartitions2 = new List[DEFFAULT_PARTITION_NUMBER];
	private static double[] probability = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

	@BeforeClass
	public static void init() {
		// initiate source data set.
		source = new ArrayList<Double>(SOURCE_SIZE);
		for (int i = 0; i < SOURCE_SIZE; i++) {
			source.add((double) i);
		}
		
		ksTest = new KolmogorovSmirnovTest();
		
		source2 = new ArrayList<WeightedData<Double>>(SOURCE_SIZE);
		for (int i =0; i < SOURCE_SIZE; i++) {
			source2.add(new WeightedData<Double>(probability[i % probability.length], (double) (i % probability.length)));
		}
	}

	private void initSourcePartition() {
		for (int i=0; i<DEFFAULT_PARTITION_NUMBER; i++) {
			sourcePartitions[i] = new LinkedList<Double>();
		}
		for (int i = 0; i< SOURCE_SIZE; i++) {
			int index = i % DEFFAULT_PARTITION_NUMBER;
			sourcePartitions[index].add((double) i);
		}
		
		for (int i=0; i < DEFFAULT_PARTITION_NUMBER; i++) {
			sourcePartitions2[i] = new LinkedList<WeightedData<Double>>();
		}
		
		for (int i = 0; i < SOURCE_SIZE / probability.length; i++) {
			int index = i % DEFFAULT_PARTITION_NUMBER;
			for (int j = 0; j < probability.length; j++) {
				sourcePartitions2[index].add(new WeightedData<Double>(probability[j % probability.length], (double) (j % probability.length)));
			}
		}
	}
	
	@Test(expected = java.lang.IllegalArgumentException.class)
	public void testBernoulliSamplerWithUnexpectedFraction1() {
		verifySamplerFraction(-1, false);
	}
	
	@Test(expected = java.lang.IllegalArgumentException.class)
	public void testBernoulliSamplerWithUnexpectedFraction2() {
		verifySamplerFraction(2, false);
	}
	
	@Test
	public void testBernoulliSamplerFraction() {
		verifySamplerFraction(0.01, false);
		verifySamplerFraction(0.05, false);
		verifySamplerFraction(0.1, false);
		verifySamplerFraction(0.3, false);
		verifySamplerFraction(0.5, false);
		verifySamplerFraction(0.854, false);
		verifySamplerFraction(0.99, false);
	}
	
	@Test
	public void testBernoulliSamplerDuplicateElements() {
		verifyRandomSamplerDuplicateElements(new BernoulliSampler<Double>(0.01));
		verifyRandomSamplerDuplicateElements(new BernoulliSampler<Double>(0.1));
		verifyRandomSamplerDuplicateElements(new BernoulliSampler<Double>(0.5));
	}
	
	@Test(expected = java.lang.IllegalArgumentException.class)
	public void testPoissonSamplerWithUnexpectedFraction1() {
		verifySamplerFraction(-1, true);
	}
	
	@Test
	public void testPoissonSamplerFraction() {
		verifySamplerFraction(0.01, true);
		verifySamplerFraction(0.05, true);
		verifySamplerFraction(0.1, true);
		verifySamplerFraction(0.5, true);
		verifySamplerFraction(0.854, true);
		verifySamplerFraction(0.99, true);
		verifySamplerFraction(1.5, true);
	}
	
	@Test(expected = java.lang.IllegalArgumentException.class)
	public void testReservoirSamplerUnexpectedSize1() {
		verifySamplerFixedSampleSize(-1, true);
	}
	
	@Test(expected = java.lang.IllegalArgumentException.class)
	public void testReservoirSamplerUnexpectedSize2() {
		verifySamplerFixedSampleSize(-1, false);
	}
	
	@Test
	public void testBernoulliSamplerDistribution() {
		verifyBernoulliSampler(0.01d);
		verifyBernoulliSampler(0.05d);
		verifyBernoulliSampler(0.1d);
		verifyBernoulliSampler(0.5d);
	}
	
	@Test
	public void testPoissonSamplerDistribution() {
		verifyPoissonSampler(0.01d);
		verifyPoissonSampler(0.05d);
		verifyPoissonSampler(0.1d);
		verifyPoissonSampler(0.5d);
	}
	
	@Test
	public void testReservoirSamplerSampledSize() {
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
	
	@Test
	public void testReservoirSamplerSampledSize2() {
		RandomSampler<Double> sampler = new ReservoirSamplerWithoutReplacement<Double>(20000);
		Iterator<Double> sampled = sampler.sample(source.iterator());
		assertTrue("ReservoirSamplerWithoutReplacement sampled output size should not beyond the source size.", getSize(sampled) == SOURCE_SIZE);
	}
	
	@Test
	public void testReservoirSamplerDuplicateElements() {
		verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<Double>(100));
		verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<Double>(1000));
		verifyRandomSamplerDuplicateElements(new ReservoirSamplerWithoutReplacement<Double>(5000));
	}
	
	@Test
	public void testReservoirSamplerWithoutReplacement() {
		verifyReservoirSamplerWithoutReplacement(100, false);
		verifyReservoirSamplerWithoutReplacement(500, false);
		verifyReservoirSamplerWithoutReplacement(1000, false);
		verifyReservoirSamplerWithoutReplacement(5000, false);
	}
	
	@Test
	public void testReservoirSamplerWithReplacement() {
		verifyReservoirSamplerWithReplacement(100, false);
		verifyReservoirSamplerWithReplacement(500, false);
		verifyReservoirSamplerWithReplacement(1000, false);
		verifyReservoirSamplerWithReplacement(5000, false);
	}

	@Test
	public void testWeightedRandomSamplingWithoutReplacement() {
		verifyWeightedRandomSamplingWithoutReplacement(100, false);
		verifyWeightedRandomSamplingWithoutReplacement(500, false);
		verifyWeightedRandomSamplingWithoutReplacement(1000, false);
		verifyWeightedRandomSamplingWithoutReplacement(5000, false);
	}

	@Test
	public void testWeightedRandomSamplingWithReplacement() {
		verifyWeightedRandomSamplingWithReplacement(100, false);
		verifyWeightedRandomSamplingWithReplacement(500, false);
		verifyWeightedRandomSamplingWithReplacement(1000, false);
		verifyWeightedRandomSamplingWithReplacement(5000, false);
	}

	@Test
	public void testReservoirSamplerWithMultiSourcePartitions1() {
		initSourcePartition();

		verifyReservoirSamplerWithoutReplacement(100, true);
		verifyReservoirSamplerWithoutReplacement(500, true);
		verifyReservoirSamplerWithoutReplacement(1000, true);
		verifyReservoirSamplerWithoutReplacement(5000, true);
	}

	@Test
	public void testReservoirSamplerWithMultiSourcePartitions2() {
		initSourcePartition();

		verifyReservoirSamplerWithReplacement(100, true);
		verifyReservoirSamplerWithReplacement(500, true);
		verifyReservoirSamplerWithReplacement(1000, true);
		verifyReservoirSamplerWithReplacement(5000, true);
	}

	@Test
	public void testWeightedRandomSamplingOnPartitions1() {
		initSourcePartition();

		verifyWeightedRandomSamplingWithoutReplacement(100, true);
		verifyWeightedRandomSamplingWithoutReplacement(500, true);
		verifyWeightedRandomSamplingWithoutReplacement(1000, true);
		verifyWeightedRandomSamplingWithoutReplacement(5000, true);
	}

	@Test
	public void testWeightedRandomSamplingOnPartitions2() {
		initSourcePartition();

		verifyWeightedRandomSamplingWithReplacement(100, true);
		verifyWeightedRandomSamplingWithReplacement(500, true);
		verifyWeightedRandomSamplingWithReplacement(1000, true);
		verifyWeightedRandomSamplingWithReplacement(5000, true);
	}

	/*
	 * Sample with fixed size, verify whether the sampled result size equals to input size.
	 */
	private void verifySamplerFixedSampleSize(int numSample, boolean withReplacement) {
		RandomSampler<Double> sampler;
		if (withReplacement) {
			sampler = new ReservoirSamplerWithReplacement<Double>(numSample);
		} else {
			sampler = new ReservoirSamplerWithoutReplacement<Double>(numSample);
		}
		Iterator<Double> sampled = sampler.sample(source.iterator());
		assertEquals(numSample, getSize(sampled));
	}

	/*
	 * Sample with fraction, and verify whether the sampled result close to input fraction.
	 */
	private void verifySamplerFraction(double fraction, boolean withReplacement) {
		RandomSampler<Double> sampler;
		if (withReplacement) {
			sampler = new PoissonSampler<Double>(fraction);
		} else {
			sampler = new BernoulliSampler<Double>(fraction);
		}
		
		// take 20 times sample, and take the average result size for next step comparison.
		int totalSampledSize = 0;
		double sampleCount = 20;
		for (int i = 0; i < sampleCount; i++) {
			totalSampledSize += getSize(sampler.sample(source.iterator()));
		}
		double resultFraction = totalSampledSize / ((double) SOURCE_SIZE * sampleCount);
		assertTrue(String.format("expected fraction: %f, result fraction: %f", fraction, resultFraction), Math.abs((resultFraction - fraction) / fraction) < 0.2);
	}

	/*
	 * Test sampler without replacement, and verify that there should not exist any duplicate element in sampled result.
	 */
	private void verifyRandomSamplerDuplicateElements(final RandomSampler<Double> sampler) {
		List<Double> list = Lists.newLinkedList(new Iterable<Double>() {
			@Override
			public Iterator<Double> iterator() {
				return sampler.sample(source.iterator());
			}
		});
		Set<Double> set = Sets.newHashSet(list);
		assertTrue("There should not have duplicate element for sampler without replacement.", list.size() == set.size());
	}
	
	private int getSize(Iterator iterator) {
		int size = 0;
		while (iterator.hasNext()) {
			iterator.next();
			size++;
		}
		return size;
	}
	
	private void verifyBernoulliSampler(double fraction) {
		BernoulliSampler<Double> sampler = new BernoulliSampler<Double>(fraction);
		verifyRandomSamplerWithFraction(fraction, sampler, true);
		verifyRandomSamplerWithFraction(fraction, sampler, false);
	}
	
	private void verifyPoissonSampler(double fraction) {
		PoissonSampler<Double> sampler = new PoissonSampler<Double>(fraction);
		verifyRandomSamplerWithFraction(fraction, sampler, true);
		verifyRandomSamplerWithFraction(fraction, sampler, false);
	}
	
	private void verifyReservoirSamplerWithReplacement(int numSamplers, boolean sampleOnPartitions) {
		ReservoirSamplerWithReplacement<Double> sampler = new ReservoirSamplerWithReplacement<Double>(numSamplers);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions);
	}
	
	private void verifyReservoirSamplerWithoutReplacement(int numSamplers, boolean sampleOnPartitions) {
		ReservoirSamplerWithoutReplacement<Double> sampler = new ReservoirSamplerWithoutReplacement<Double>(numSamplers);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions);
	}

	private void verifyWeightedRandomSamplingWithoutReplacement(int numSamplers, boolean sampleOnPartitions) {
		WeightedRandomSamplerWithoutReplacement<WeightedData<Double>> sampler = new WeightedRandomSamplerWithoutReplacement<WeightedData<Double>>(numSamplers);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions, true);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions, true);
	}

	private void verifyWeightedRandomSamplingWithReplacement(int numSamplers, boolean sampleOnPartitions) {
		WeightedRandomSamplerWithReplacement<WeightedData<Double>> sampler = new WeightedRandomSamplerWithReplacement<WeightedData<Double>>(numSamplers);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions, true);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions, true);
	}

	/*
	 * Verify whether random sampler sample with fraction from source data randomly. There are two default sample, one is
	 * sampled from source data with certain interval, the other is sampled only from the first half region of source data,
	 * If random sampler select elements randomly from source, it would distributed well-proportioned on source data as well,
	 * so the K-S Test result would accept the first one, while reject the second one.
	 */
	private void verifyRandomSamplerWithFraction(double fraction, RandomSampler sampler, boolean withDefaultSampler) {
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
	private void verifyRandomSamplerWithSampleSize(int sampleSize, RandomSampler sampler, boolean withDefaultSampler, boolean sampleWithPartitions, boolean withWeighted) {
		double[] baseSample = null;
		if (withDefaultSampler) {
			if (!withWeighted) {
				baseSample = getDefaultSampler(sampleSize);
			}
			else {
				baseSample = getDefaultWeightedSampler();
			}
		} else {
			if (!withWeighted) {
				baseSample = getWrongSampler(sampleSize);
			}
			else {
				baseSample = getWrongWeightedSampler();
			}
		}
		
		verifyKSTest(sampler, baseSample, withDefaultSampler, sampleWithPartitions, withWeighted);
	}
	
	private void verifyRandomSamplerWithSampleSize(int sampleSize, RandomSampler sampler, boolean withDefaultSampler, boolean sampleWithPartitions) {
		this.verifyRandomSamplerWithSampleSize(sampleSize, sampler, withDefaultSampler, sampleWithPartitions, false);
	}

	private void verifyKSTest(RandomSampler sampler, double[] defaultSampler, boolean expectSuccess) {
		verifyKSTest(sampler, defaultSampler, expectSuccess, false, false);
	}

	private void verifyKSTest(RandomSampler sampler, double[] defaultSampler, boolean expectSuccess, boolean sampleOnPartitions, boolean withWeighted) {
		double[] sampled = null;
		if (!withWeighted) {
			sampled = getSampledOutput(sampler,sampleOnPartitions);
		}
		else {
			sampled = getWeightedSampledOutput(sampler, sampleOnPartitions);
		}
		double pValue = ksTest.kolmogorovSmirnovStatistic(sampled, defaultSampler);
		double dValue = getDValue(sampled.length, defaultSampler.length);
		if (expectSuccess) {
			assertTrue(String.format("KS test result with p value(%f), d value(%f)", pValue, dValue), pValue <= dValue);
		} else {
			assertTrue(String.format("KS test result with p value(%f), d value(%f)", pValue, dValue), pValue > dValue);
		}
	}
	
	private double[] getSampledOutput(RandomSampler<Double> sampler, boolean sampleOnPartitions) {
		Iterator<Double> sampled = null;
		if (sampleOnPartitions) {
			DistributedRandomSampler<Double> reservoirRandomSampler = (DistributedRandomSampler<Double>)sampler;
			List<IntermediateSampleData<Double>> intermediateResult = Lists.newLinkedList();
			for (int i=0; i<DEFFAULT_PARTITION_NUMBER; i++) {
				Iterator<IntermediateSampleData<Double>> partialIntermediateResult = reservoirRandomSampler.sampleInPartition(sourcePartitions[i].iterator());
				while (partialIntermediateResult.hasNext()) {
					intermediateResult.add(partialIntermediateResult.next());
				}
			}
			sampled = reservoirRandomSampler.sampleInCoordinator(intermediateResult.iterator());
		} else {
			sampled = sampler.sample(source.iterator());
		}
		List<Double> list = Lists.newArrayList();
		while (sampled.hasNext()) {
			list.add(sampled.next());
		}
		double[] result = transferFromListToArrayWithOrder(list);
		return result;
	}

	private double[] getWeightedSampledOutput(RandomSampler<WeightedData<Double>> sampler, boolean sampleOnPartitions) {
		Iterator<WeightedData<Double>> sampled = null;
		if (sampleOnPartitions) {
			DistributedRandomSampler<WeightedData<Double>> reservoirRandomSampler = (DistributedRandomSampler<WeightedData<Double>>)sampler;
			List<IntermediateSampleData<WeightedData<Double>>> intermediateResult = Lists.newLinkedList();
			for (int i = 0; i < DEFFAULT_PARTITION_NUMBER; i++) {
				Iterator<IntermediateSampleData<WeightedData<Double>>> partialIntermediateResult = reservoirRandomSampler.sampleInPartition(sourcePartitions2[i].iterator());
				while (partialIntermediateResult.hasNext()) {
					intermediateResult.add(partialIntermediateResult.next());
				}
			}
			sampled = reservoirRandomSampler.sampleInCoordinator(intermediateResult.iterator());
		} else {
			sampled = sampler.sample(source2.iterator());
		}
		List<Double> list = Lists.newArrayList();
		while (sampled.hasNext()) {
			list.add(sampled.next().getElement());
		}
		double[] result = countAndCalculateProbability(list);
		return result;
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

	private double[] countAndCalculateProbability(List<Double> list) {
		double[] result = new double[probability.length];
		for (int i = 0; i < list.size(); i++) {
			result[list.get(i).intValue()]++;
		}
		for (int i = 0; i < result.length; i++) {
			result[i] /= list.size();
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
		int size = fixSize;
		double step = SOURCE_SIZE / (double) fixSize;
		double[] defaultSampler = new double[size];
		for (int i = 0; i < size; i++) {
			defaultSampler[i] = Math.round(step * i);
		}
		
		return defaultSampler;
	}
	
	/*
	 * Calculate the probability of every element
	 */
	private double[] getDefaultWeightedSampler() {
		double[] defaultWeightedSampler = new double[probability.length];
		for (int i = 0; i < probability.length; i++) {
			defaultWeightedSampler[i] = probability[i] / 5.5;
		}

		return defaultWeightedSampler;
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
		int size = fixSize;
		double[] wrongSampler = new double[size];
		for (int i = 0; i < size; i++) {
			wrongSampler[i] = (double) i % halfSourceSize;
		}
		
		return wrongSampler;
	}
	
	/*
	 * Build a failed weighted sample distribution which contains elements with wrong probability .
	 */
	private double[] getWrongWeightedSampler() {
		double[] wrongSample = new double[10];
		for (int i = 0; i < wrongSample.length; i++) {
			wrongSample[i] = 1;
		}
		return wrongSample;
	}
	
	/*
	 * Calculate the D value of K-S test for p-value 0.01, m and n are the sample size
	 */
	private double getDValue(int m, int n) {
		Preconditions.checkArgument(m > 0, "input sample size should be positive.");
		Preconditions.checkArgument(n > 0, "input sample size should be positive.");
		double first = (double) m;
		double second = (double) n;
		return 1.63 * Math.sqrt((first + second) / (first * second));
	}
}
