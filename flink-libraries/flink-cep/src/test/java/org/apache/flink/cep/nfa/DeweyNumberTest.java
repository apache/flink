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

package org.apache.flink.cep.nfa;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeweyNumber}.
 */
public class DeweyNumberTest extends TestLogger {

	@Test
	public void testDeweyNumberGeneration() {
		DeweyNumber start = new DeweyNumber(1);
		DeweyNumber increased = start.increase();
		DeweyNumber increaseAddStage = increased.addStage();
		DeweyNumber startAddStage = start.addStage();
		DeweyNumber startAddStageIncreased = startAddStage.increase();
		DeweyNumber startAddStageIncreasedAddStage = startAddStageIncreased.addStage();

		assertEquals(DeweyNumber.fromString("1"), start);
		assertEquals(DeweyNumber.fromString("2"), increased);
		assertEquals(DeweyNumber.fromString("2.0"), increaseAddStage);
		assertEquals(DeweyNumber.fromString("1.0"), startAddStage);
		assertEquals(DeweyNumber.fromString("1.1"), startAddStageIncreased);
		assertEquals(DeweyNumber.fromString("1.1.0"), startAddStageIncreasedAddStage);

		assertTrue(startAddStage.isCompatibleWith(start));
		assertTrue(startAddStageIncreased.isCompatibleWith(startAddStage));
		assertTrue(startAddStageIncreasedAddStage.isCompatibleWith(startAddStageIncreased));
		assertFalse(startAddStageIncreasedAddStage.isCompatibleWith(startAddStage));
		assertFalse(increaseAddStage.isCompatibleWith(startAddStage));
		assertFalse(startAddStage.isCompatibleWith(increaseAddStage));
		assertFalse(startAddStageIncreased.isCompatibleWith(startAddStageIncreasedAddStage));
	}
}
