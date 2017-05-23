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

package org.apache.flink.storm.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.storm.util.AbstractTest;

import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;

/**
 * Tests for the FlinkOutputFieldsDeclarer.
 */
public class FlinkOutputFieldsDeclarerTest extends AbstractTest {

	@Test
	public void testNull() {
		Assert.assertNull(new FlinkOutputFieldsDeclarer().getOutputType(null));
	}

	@Test
	public void testDeclare() {
		for (int i = 0; i < 2; ++i) { // test case: simple / non-direct
			for (int j = 1; j < 2; ++j) { // number of streams
				for (int k = 0; k <= 24; ++k) { // number of attributes
					this.runDeclareTest(i, j, k);
				}
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareSimpleToManyAttributes() {
		this.runDeclareTest(0, this.r.nextBoolean() ? 1 : 2, 25);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareNonDirectToManyAttributes() {
		this.runDeclareTest(1, this.r.nextBoolean() ? 1 : 2, 25);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareDefaultStreamToManyAttributes() {
		this.runDeclareTest(2, this.r.nextBoolean() ? 1 : 2, 25);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareFullToManyAttributes() {
		this.runDeclareTest(3, this.r.nextBoolean() ? 1 : 2, 25);
	}

	private void runDeclareTest(final int testCase, final int numberOfStreams,
			final int numberOfAttributes) {
		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();

		String[] streams = null;
		if (numberOfStreams > 1 || r.nextBoolean()) {
			streams = new String[numberOfStreams];
			for (int i = 0; i < numberOfStreams; ++i) {
				streams[i] = "stream" + i;
			}
		}

		final String[] attributes = new String[numberOfAttributes];
		for (int i = 0; i < attributes.length; ++i) {
			attributes[i] = "a" + i;
		}

		switch (testCase) {
		case 0:
			this.declareSimple(declarer, streams, attributes);
			break;
		default:
			this.declareNonDirect(declarer, streams, attributes);
		}

		if (streams == null) {
			streams = new String[] { Utils.DEFAULT_STREAM_ID };
		}

		for (String stream : streams) {
			final TypeInformation<?> type = declarer.getOutputType(stream);

			Assert.assertEquals(numberOfAttributes + 1, type.getArity());
			Assert.assertTrue(type.isTupleType());
		}
	}

	private void declareSimple(final FlinkOutputFieldsDeclarer declarer, final String[] streams,
			final String[] attributes) {

		if (streams != null) {
			for (String stream : streams) {
				declarer.declareStream(stream, new Fields(attributes));
			}
		} else {
			declarer.declare(new Fields(attributes));
		}
	}

	private void declareNonDirect(final FlinkOutputFieldsDeclarer declarer, final String[] streams,
			final String[] attributes) {

		if (streams != null) {
			for (String stream : streams) {
				declarer.declareStream(stream, false, new Fields(attributes));
			}
		} else {
			declarer.declare(false, new Fields(attributes));
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUndeclared() {
		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
		declarer.getOutputType("unknownStreamId");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirect() {
		new FlinkOutputFieldsDeclarer().declare(true, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirect2() {
		new FlinkOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, true, null);
	}

	@Test
	public void testGetGroupingFieldIndexes() {
		final int numberOfAttributes = 5 + this.r.nextInt(20);
		final String[] attributes = new String[numberOfAttributes];
		for (int i = 0; i < numberOfAttributes; ++i) {
			attributes[i] = "a" + i;
		}

		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
		declarer.declare(new Fields(attributes));

		final int numberOfKeys = 1 + this.r.nextInt(24);
		final LinkedList<String> groupingFields = new LinkedList<String>();
		final boolean[] indexes = new boolean[numberOfAttributes];

		for (int i = 0; i < numberOfAttributes; ++i) {
			if (this.r.nextInt(25) < numberOfKeys) {
				groupingFields.add(attributes[i]);
				indexes[i] = true;
			} else {
				indexes[i] = false;
			}
		}

		final int[] expectedResult = new int[groupingFields.size()];
		int j = 0;
		for (int i = 0; i < numberOfAttributes; ++i) {
			if (indexes[i]) {
				expectedResult[j++] = i;
			}
		}

		final int[] result = declarer.getGroupingFieldIndexes(Utils.DEFAULT_STREAM_ID,
				groupingFields);

		Assert.assertEquals(expectedResult.length, result.length);
		for (int i = 0; i < expectedResult.length; ++i) {
			Assert.assertEquals(expectedResult[i], result[i]);
		}
	}

}
