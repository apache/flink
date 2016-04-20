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
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

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
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test">Kolmogorov Smirnov test</a>
 */
public class VeryFastReservoirSamplingTest {
	private final static int SOURCE_SIZE = 10000;
	private static KolmogorovSmirnovTest ksTest;
	private static List<Double> source;
	private final static int DEFFAULT_PARTITION_NUMBER=10;
	private List<Double>[] sourcePartitions = new List[DEFFAULT_PARTITION_NUMBER];

	@BeforeClass
	public static void init() {
		// initiate source data set.
		source = new ArrayList<Double>(SOURCE_SIZE);
		for (int i = 0; i < SOURCE_SIZE; i++) {
			source.add((double) i);
		}

		ksTest = new KolmogorovSmirnovTest();
	}

	private void initSourcePartition() {
		for (int i=0; i<DEFFAULT_PARTITION_NUMBER; i++) {
			sourcePartitions[i] = new LinkedList<Double>();
		}
		for (int i = 0; i< SOURCE_SIZE; i++) {
			int index = i % DEFFAULT_PARTITION_NUMBER;
			sourcePartitions[index].add((double)i);
		}
	}
//
//	@Test(expected = java.lang.IllegalArgumentException.class)
//	public void testBernoulliSamplerWithUnexpectedFraction1() {
//		verifySamplerFraction(-1, false);
//	}
//
//	@Test(expected = java.lang.IllegalArgumentException.class)
//	public void testBernoulliSamplerWithUnexpectedFraction2() {
//		verifySamplerFraction(2, false);
//	}

	@Test
	public void testVeryFastReservoirSampler() {
		initSourcePartition();

		verifyVeryFastReservoirSampler(10, false);
	}
	@Test
	public void testVeryFastReservoirSamplerWithMultiSourcePartitions() {
		initSourcePartition();

		verifyVeryFastReservoirSampler(100, true);
	}

	private int getSize(Iterator iterator) {
		int size = 0;
		while (iterator.hasNext()) {
			iterator.next();
			size++;
		}
		return size;
	}

	private void verifyVeryFastReservoirSampler(int numSamplers, boolean sampleOnPartitions) {
		VeryFastReservoirSampler<Double> sampler = new VeryFastReservoirSampler<Double>(numSamplers);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, true, sampleOnPartitions);
		verifyRandomSamplerWithSampleSize(numSamplers, sampler, false, sampleOnPartitions);
	}

	/*
	 * Verify whether random sampler sample with fixed size from source data randomly. There are two default sample, one is
	 * sampled from source data with certain interval, the other is sampled only from the first half region of source data,
	 * If random sampler select elements randomly from source, it would distributed well-proportioned on source data as well,
	 * so the K-S Test result would accept the first one, while reject the second one.
	 */
	private void verifyRandomSamplerWithSampleSize(int sampleSize, RandomSampler sampler, boolean withDefaultSampler, boolean sampleWithPartitions) {
		double[] baseSample;
		if (withDefaultSampler) {
			baseSample = getDefaultSampler(sampleSize);
		} else {
			baseSample = getWrongSampler(sampleSize);
		}

		verifyKSTest(sampler, baseSample, withDefaultSampler, sampleWithPartitions);
	}

	private void verifyKSTest(RandomSampler sampler, double[] defaultSampler, boolean expectSuccess, boolean sampleOnPartitions) {
		double[] sampled = getSampledOutput(sampler, sampleOnPartitions);
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
