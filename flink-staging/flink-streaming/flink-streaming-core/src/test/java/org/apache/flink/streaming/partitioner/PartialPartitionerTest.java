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
package org.apache.flink.streaming.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class PartialPartitionerTest {
	
	private PartialPartitioner<Tuple> partialPartitioner;
	private StreamRecord<Tuple> streamRecord1 = new StreamRecord<Tuple>()
			.setObject(new Tuple2<String, Integer>("test", 0));
	private SerializationDelegate<StreamRecord<Tuple>> sd1 = new SerializationDelegate<StreamRecord<Tuple>>(null);
	int numOfOutChannels = 10;
	
	@Before
	public void setPartitioner() {
		partialPartitioner = new PartialPartitioner<Tuple>(new KeySelector<Tuple, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple value) throws Exception {
				return value.getField(0);
			}
		});
	}
	
	@Test
	public void testSelectChannelsLength() {
		sd1.setInstance(streamRecord1);
		assertEquals(1, partialPartitioner.selectChannels(sd1, numOfOutChannels).length);
	}
	
	@Test
	public void testSelectChannels() {
		sd1.setInstance(streamRecord1);
		
		int choice1 = partialPartitioner.selectChannels(sd1, numOfOutChannels)[0];
		int choice2 = partialPartitioner.selectChannels(sd1, numOfOutChannels)[0];	
		assertNotEquals(choice1,choice2);
	}
	
}
