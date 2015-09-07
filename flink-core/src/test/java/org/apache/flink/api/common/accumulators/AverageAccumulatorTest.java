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

package org.apache.flink.api.common.accumulators;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AverageAccumulatorTest {

	@Test
	public void testGet() {
		AverageAccumulator average = new AverageAccumulator();
		assertEquals(Double.valueOf(0), average.getLocalValue());
	}

	@Test
	public void testAdd() {
		AverageAccumulator average = new AverageAccumulator();
		int i1;
		for (i1 = 0; i1 < 10; i1++) {
			average.add(i1);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		Integer i2;
		for (i2 = 0; i2 < 10; i2++) {
			average.add(i2);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		long i3;
		for (i3 = 0; i3 < 10; i3++) {
			average.add(i3);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		Long i4;
		for (i4 = 0L; i4 < 10; i4++) {
			average.add(i4);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		double i5;
		for (i5 = 0; i5 < 10; i5++) {
			average.add(i5);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		Double i6;
		for (i6 = 0.0; i6 < 10; i6++) {
			average.add(i6);
		}
		assertEquals(Double.valueOf(4.5), average.getLocalValue());
		average.resetLocal();

		assertEquals(Double.valueOf(0), average.getLocalValue());
	}

	@Test
	public void testMergeSuccess() {
		AverageAccumulator average = new AverageAccumulator();
		AverageAccumulator averageNew = new AverageAccumulator();
		average.add(1);
		averageNew.add(2);
		average.merge(averageNew);
		assertEquals(Double.valueOf(1.5), average.getLocalValue());
	}

	@Test
	public void testMergeFailed() {
		AverageAccumulator average = new AverageAccumulator();
		Accumulator<Double, Double> averageNew = null;
		average.add(1);
		try {
			average.merge(averageNew);
		} catch (Exception e) {
			assertTrue(e.toString().indexOf("The merged accumulator must be AverageAccumulator.") != -1);
			return;
		}
		fail();
	}

	@Test
	public void testClone() {
		AverageAccumulator average = new AverageAccumulator();
		average.add(1);
		AverageAccumulator averageNew = average.clone();
		assertEquals(Double.valueOf(1), averageNew.getLocalValue());
	}
}
