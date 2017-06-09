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

package org.apache.flink.storm.wrappers;

import org.apache.flink.storm.util.AbstractTest;

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Tests for the SetupOutputFieldsDeclarer.
 */
public class SetupOutputFieldsDeclarerTest extends AbstractTest {

	@Test
	public void testDeclare() {
		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();

		int numberOfAttributes = this.r.nextInt(26);
		declarer.declare(createSchema(numberOfAttributes));
		Assert.assertEquals(1, declarer.outputSchemas.size());
		Assert.assertEquals(numberOfAttributes, declarer.outputSchemas.get(Utils.DEFAULT_STREAM_ID)
				.intValue());

		final String sid = "streamId";
		numberOfAttributes = this.r.nextInt(26);
		declarer.declareStream(sid, createSchema(numberOfAttributes));
		Assert.assertEquals(2, declarer.outputSchemas.size());
		Assert.assertEquals(numberOfAttributes, declarer.outputSchemas.get(sid).intValue());
	}

	private Fields createSchema(final int numberOfAttributes) {
		final ArrayList<String> schema = new ArrayList<String>(numberOfAttributes);
		for (int i = 0; i < numberOfAttributes; ++i) {
			schema.add("a" + i);
		}
		return new Fields(schema);
	}

	@Test
	public void testDeclareDirect() {
		new SetupOutputFieldsDeclarer().declare(false, new Fields());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirectFail() {
		new SetupOutputFieldsDeclarer().declare(true, new Fields());
	}

	@Test
	public void testDeclareStream() {
		new SetupOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, new Fields());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareStreamFail() {
		new SetupOutputFieldsDeclarer().declareStream(null, new Fields());
	}

	@Test
	public void testDeclareFullStream() {
		new SetupOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, false, new Fields());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareFullStreamFailNonDefaultStream() {
		new SetupOutputFieldsDeclarer().declareStream(null, false, new Fields());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareFullStreamFailDirect() {
		new SetupOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, true, new Fields());
	}

}
