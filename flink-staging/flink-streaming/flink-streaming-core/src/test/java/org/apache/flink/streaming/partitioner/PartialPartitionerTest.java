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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class PartialPartitionerTest {
	
	private PartialPartitioner<Tuple2<String, Integer>> partialPartitioner;
	private StreamRecord<Tuple2<String, Integer>> streamRecord1 = new StreamRecord<Tuple2<String, Integer>>()
			.setObject(new Tuple2<String, Integer>("test", 0));
	private StreamRecord<Tuple2<String, Integer>> streamRecord2 = new StreamRecord<Tuple2<String, Integer>>()
			.setObject(new Tuple2<String, Integer>("test", 42));
	private StreamRecord<Tuple2<String, Integer>> streamRecord3 = new StreamRecord<Tuple2<String, Integer>>()
			.setObject(new Tuple2<String, Integer>("test", 33));
	private SerializationDelegate<StreamRecord<Tuple2<String, Integer>>> sd1 = new SerializationDelegate<StreamRecord<Tuple2<String, Integer>>>(null);
	private SerializationDelegate<StreamRecord<Tuple2<String, Integer>>> sd2 = new SerializationDelegate<StreamRecord<Tuple2<String, Integer>>>(null);
	private SerializationDelegate<StreamRecord<Tuple2<String, Integer>>> sd3 = new SerializationDelegate<StreamRecord<Tuple2<String, Integer>>>(null);
	private static int numOfOutChannels = 10;
    
	@Before
	public void setPartitioner() {
		partialPartitioner = new PartialPartitioner<Tuple2<String, Integer>>(new KeySelector<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.getField(0);
			}
		}, numOfOutChannels);
	}
	
	@Test
	public void testSelectChannelsLength() {
		sd1.setInstance(streamRecord1);
		assertEquals(1, partialPartitioner.selectChannels(sd1, numOfOutChannels).length);
	}
	
	@Test
	public void testSelectChannels() {
		sd1.setInstance(streamRecord1);
		sd2.setInstance(streamRecord2);
		sd3.setInstance(streamRecord3);
		
		int choice1 = partialPartitioner.selectChannels(sd1, numOfOutChannels)[0];
		int choice2 = partialPartitioner.selectChannels(sd2, numOfOutChannels)[0];	
		assertNotEquals(choice1,choice2);
				
		int choice3 = partialPartitioner.selectChannels(sd3, numOfOutChannels)[0];
		assertNotEquals(choice3,choice2);
		assertEquals(choice3,choice1);
	}
	
}
