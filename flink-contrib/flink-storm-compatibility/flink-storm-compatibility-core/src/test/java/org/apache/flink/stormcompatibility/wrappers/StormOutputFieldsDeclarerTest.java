/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.stormcompatibility.wrappers;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class StormOutputFieldsDeclarerTest extends AbstractTest {

	@Test
	public void testDeclare() {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();

		Assert.assertEquals(-1, declarer.getNumberOfAttributes());

		final int numberOfAttributes = 1 + this.r.nextInt(25);
		final ArrayList<String> schema = new ArrayList<String>(numberOfAttributes);
		for (int i = 0; i < numberOfAttributes; ++i) {
			schema.add("a" + i);
		}
		declarer.declare(new Fields(schema));
		Assert.assertEquals(numberOfAttributes, declarer.getNumberOfAttributes());
	}

	@Test
	public void testDeclareDirect() {
		new StormOutputFieldsDeclarer().declare(false, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirectFail() {
		new StormOutputFieldsDeclarer().declare(true, null);
	}

	@Test
	public void testDeclareStream() {
		new StormOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareStreamFail() {
		new StormOutputFieldsDeclarer().declareStream(null, null);
	}

	@Test
	public void testDeclareFullStream() {
		new StormOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, false, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareFullStreamFailNonDefaultStream() {
		new StormOutputFieldsDeclarer().declareStream(null, false, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareFullStreamFailDirect() {
		new StormOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, true, null);
	}

}
