/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for VectorStatCol.
 */
public class VectorStatColTest {

	@Test
	public void test() {
		VectorStatCol stat = new VectorStatCol();
		stat.visit(1.0);
		stat.visit(-2.0);
		stat.visit(Double.NaN);

		assertEquals(1.0, stat.max, 10e-6);
		assertEquals(-2.0, stat.min, 10e-6);
		assertEquals(5.0, stat.squareSum, 10e-6);
		assertEquals(3.0, stat.normL1, 10e-6);
		assertEquals(2.0, stat.numNonZero, 10e-6);
		assertEquals(-1.0, stat.sum, 10e-6);
		assertEquals(-0.5, stat.mean(2), 10e-6);
		assertEquals(4.5, stat.variance(2), 10e-6);
		assertEquals(2.12132, stat.standardDeviation(2), 10e-6);
	}

}
