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

import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.HashSet;

import static java.util.Collections.singleton;
import static org.mockito.Mockito.mock;

/**
 * Tests for the WrapperSetupHelper.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(WrapperSetupHelper.class)
@PowerMockIgnore({"javax.*", "org.apache.log4j.*"})
public class WrapperSetupHelperTest extends AbstractTest {

	@Test
	public void testEmptyDeclarerBolt() {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		Assert.assertEquals(new HashMap<String, Integer>(),
				WrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRawType() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		WrapperSetupHelper.getNumberOfAttributes(boltOrSpout,
				new HashSet<String>(singleton(Utils.DEFAULT_STREAM_ID)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testToManyAttributes() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		WrapperSetupHelper.getNumberOfAttributes(boltOrSpout, null);
	}

	@Test
	public void testTupleTypes() throws Exception {
		for (int i = -1; i < 26; ++i) {
			this.testTupleTypes(i);
		}
	}

	private void testTupleTypes(final int numberOfAttributes) throws Exception {
		String[] schema;
		if (numberOfAttributes == -1) {
			schema = new String[1];
		} else {
			schema = new String[numberOfAttributes];
		}
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}

		IComponent boltOrSpout;
		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final SetupOutputFieldsDeclarer declarer = new SetupOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(SetupOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put(Utils.DEFAULT_STREAM_ID, numberOfAttributes);

		Assert.assertEquals(attributes, WrapperSetupHelper.getNumberOfAttributes(
				boltOrSpout,
				numberOfAttributes == -1 ? new HashSet<String>(singleton(Utils.DEFAULT_STREAM_ID)) : null));
	}
}
