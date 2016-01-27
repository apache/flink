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

package org.apache.flink.contrib.operatorstatistics.heavyhitters;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/*
* Test the structure implemented for Lossy Counting
*/

public class LossyCountingTest {

	private static final Logger LOG = LoggerFactory.getLogger(LossyCountingTest.class);

	static final double fraction = 0.01;
	static final double error = 0.005;
	static final Random r = new Random();
	static final int cardinality = 1000000;
	static final int maxScale = 100000;

	@Test
	public void testAccuracy() {

		long[] actualFreq = new long[maxScale];

		LossyCounting lossyCounting = new LossyCounting(fraction,error);

		for (int i = 0; i < cardinality; i++) {
			int value;
			if (r.nextDouble()<0.1){
				value = r.nextInt(10);
			}else{
				value = r.nextInt(maxScale);
			}
			lossyCounting.addObject(value);
			actualFreq[value]++;
		}

		LOG.debug("Size of heavy hitters: "+lossyCounting.getHeavyHitters().size());
		LOG.debug(lossyCounting.toString());

		Map<Object,Long> heavyHitters = lossyCounting.getHeavyHitters();
		long frequency = (long)Math.ceil(cardinality* fraction);
		long minFrequency = (long)Math.ceil(cardinality* (fraction-error));

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency) {
				assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i] + " Expected freq." + frequency, heavyHitters.containsKey(i));
			}
			if (heavyHitters.containsKey(i)){
				assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
				assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+heavyHitters.get(i),
						Math.abs(heavyHitters.get(i)-actualFreq[i]) < error*cardinality);
			}
		}
	}

	@Test
	public void merge() throws HeavyHitterMergeException {
		int numToMerge = 5;
		LossyCounting merged = new LossyCounting(fraction,error);
		LossyCounting[] sketches = new LossyCounting[numToMerge];

		long[] actualFreq = new long[maxScale];
		long totalCardinality = 0;

		for (int i = 0; i < numToMerge; i++) {
			sketches[i] = new LossyCounting(fraction,error);
			for (int j = 0; j < cardinality; j++) {
				int val;
				if (r.nextDouble()<0.1){
					val = r.nextInt(10);
				}else{
					val = r.nextInt(maxScale);
				}
				sketches[i].addObject(val);
				actualFreq[val]++;
				totalCardinality++;
			}
			merged.merge(sketches[i]);
		}

		System.out.println("\nMERGED\n" + merged.toString());

		Map<Object,Long> mergedHeavyHitters = merged.getHeavyHitters();
		long frequency = (long)(totalCardinality*fraction);
		long minFrequency = (long)(totalCardinality*(fraction-error));

		System.out.println("Frequency Threshold:" + frequency);
		System.out.println("False positive Threshold:" + minFrequency);
		System.out.println("Frequency of 14:" + actualFreq[14]);

		System.out.println("Real frequent items: ");

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency) {
				System.out.println(i+": "+actualFreq[i]);
				assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Expected freq."+ frequency, mergedHeavyHitters.containsKey(i));
			}
			if (mergedHeavyHitters.containsKey(i)){
				assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
				assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+mergedHeavyHitters.get(i),
						Math.abs(mergedHeavyHitters.get(i)-actualFreq[i]) < error*cardinality);
			}
		}

	}
}
