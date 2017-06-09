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

package org.apache.flink.storm.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Tests for the StormStreamSelector.
 */
public class StormStreamSelectorTest {

	@Test
	public void testSelector() {
		StormStreamSelector<Object> selector = new StormStreamSelector<Object>();
		SplitStreamType<Object> tuple = new SplitStreamType<Object>();
		Iterator<String> result;

		tuple.streamId = "stream1";
		result = selector.select(tuple).iterator();
		Assert.assertEquals("stream1", result.next());
		Assert.assertFalse(result.hasNext());

		tuple.streamId = "stream2";
		result = selector.select(tuple).iterator();
		Assert.assertEquals("stream2", result.next());
		Assert.assertFalse(result.hasNext());

		tuple.streamId = "stream1";
		result = selector.select(tuple).iterator();
		Assert.assertEquals("stream1", result.next());
		Assert.assertFalse(result.hasNext());
	}

}
