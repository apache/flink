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

import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormWrapperSetupHelperTest extends AbstractTest {

	@Test(expected = IllegalArgumentException.class)
	public void testZeroAttributesDeclarerBolt() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields());
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, this.r.nextBoolean());
	}

	@Test
	public void testEmptyDeclarerBolt() {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		Assert.assertEquals(-1, StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, this.r.nextBoolean()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRawType() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, true);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testToManyAttributes() throws Exception {
		IComponent boltOrSpout;

		if (this.r.nextBoolean()) {
			boltOrSpout = mock(IRichSpout.class);
		} else {
			boltOrSpout = mock(IRichBolt.class);
		}

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for (int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, false);
	}

	@Test
	public void testTupleTypes() throws Exception {
		for (int i = 0; i < 26; ++i) {
			this.testTupleTypes(i);
		}
	}

	private void testTupleTypes(final int numberOfAttributes) throws Exception {
		String[] schema = new String[numberOfAttributes];
		if (numberOfAttributes == 0) {
			schema = new String[1];
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

		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);

		StormWrapperSetupHelper.getNumberOfAttributes(boltOrSpout, numberOfAttributes == 0);
	}

}
