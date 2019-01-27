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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.table.plan.util.SortUtil;

import org.junit.Test;

import java.util.Random;

/**
 * Sort test for one field.
 */
public class RandomOneKeySortTest extends RandomCodegenSortTest {

	@Test
	public void test() throws Exception {
		for (int time = 0; time < 100; time++) {
			Random rnd = new Random();
			fields = new int[rnd.nextInt(9) + 1];
			for (int i = 0; i < fields.length; i++) {
				fields[i] = rnd.nextInt(types.length);
			}

			keys = new int[] {0};
			orders = new boolean[] {rnd.nextBoolean()};
			nullsIsLast = SortUtil.getNullDefaultOrders(orders);
			testInner();
		}
	}

}
