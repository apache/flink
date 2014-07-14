/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;

public class StreamCollectorTest {

	@Test
	public void testStreamCollector() {
		StreamCollector collector = new StreamCollector(10, 0);
		assertEquals(10, collector.batchSize);
	}

	@Test
	public void testCollect() {
		StreamCollector collector = new StreamCollector(2, 0);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

	}

	@Test
	public void testClose() {
	}

}
