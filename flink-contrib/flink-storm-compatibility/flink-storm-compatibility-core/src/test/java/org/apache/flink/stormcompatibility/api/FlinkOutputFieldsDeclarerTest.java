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

package org.apache.flink.stormcompatibility.api;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;

public class FlinkOutputFieldsDeclarerTest extends AbstractTest {



	@Test
	public void testDeclare() {
		for (int i = 0; i < 4; ++i) {
			for (int j = 0; j <= 25; ++j) {
				this.runDeclareTest(i, j);
			}
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareSimpleToManyAttributes() {
		this.runDeclareTest(0, 26);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareNonDirectToManyAttributes() {
		this.runDeclareTest(1, 26);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareDefaultStreamToManyAttributes() {
		this.runDeclareTest(2, 26);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDeclareFullToManyAttributes() {
		this.runDeclareTest(3, 26);
	}

	private void runDeclareTest(final int testCase, final int numberOfAttributes) {
		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();

		final String[] attributes = new String[numberOfAttributes];
		for (int i = 0; i < numberOfAttributes; ++i) {
			attributes[i] = "a" + i;
		}

		switch (testCase) {
			case 0:
				this.declareSimple(declarer, attributes);
				break;
			case 1:
				this.declareNonDirect(declarer, attributes);
				break;
			case 2:
				this.declareDefaultStream(declarer, attributes);
				break;
			default:
				this.declareFull(declarer, attributes);
		}

		final TypeInformation<?> type = declarer.getOutputType();

		if (numberOfAttributes == 0) {
			Assert.assertNull(type);
		} else {
			Assert.assertEquals(numberOfAttributes, type.getArity());
			if (numberOfAttributes == 1) {
				Assert.assertFalse(type.isTupleType());
			} else {
				Assert.assertTrue(type.isTupleType());
			}
		}
	}

	private void declareSimple(final FlinkOutputFieldsDeclarer declarer, final String[] attributes) {
		declarer.declare(new Fields(attributes));
	}

	private void declareNonDirect(final FlinkOutputFieldsDeclarer declarer, final String[] attributes) {
		declarer.declare(false, new Fields(attributes));
	}

	private void declareDefaultStream(final FlinkOutputFieldsDeclarer declarer, final String[] attributes) {
		declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields(attributes));
	}

	private void declareFull(final FlinkOutputFieldsDeclarer declarer, final String[] attributes) {
		declarer.declareStream(Utils.DEFAULT_STREAM_ID, false, new Fields(attributes));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirect() {
		new FlinkOutputFieldsDeclarer().declare(true, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareNonDefaultStrem() {
		new FlinkOutputFieldsDeclarer().declareStream("dummy", null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareDirect2() {
		new FlinkOutputFieldsDeclarer().declareStream(Utils.DEFAULT_STREAM_ID, true, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDeclareNonDefaultStrem2() {
		new FlinkOutputFieldsDeclarer().declareStream("dummy", this.r.nextBoolean(), null);
	}

	@Test
	public void testGetGroupingFieldIndexes() {
		final int numberOfAttributes = 5 + this.r.nextInt(21);
		final String[] attributes = new String[numberOfAttributes];
		for (int i = 0; i < numberOfAttributes; ++i) {
			attributes[i] = "a" + i;
		}

		final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
		declarer.declare(new Fields(attributes));

		final int numberOfKeys = 1 + this.r.nextInt(25);
		final LinkedList<String> groupingFields = new LinkedList<String>();
		final boolean[] indexes = new boolean[numberOfAttributes];

		for (int i = 0; i < numberOfAttributes; ++i) {
			if (this.r.nextInt(26) < numberOfKeys) {
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

		final int[] result = declarer.getGroupingFieldIndexes(groupingFields);

		Assert.assertEquals(expectedResult.length, result.length);
		for (int i = 0; i < expectedResult.length; ++i) {
			Assert.assertEquals(expectedResult[i], result[i]);
		}
	}

}
