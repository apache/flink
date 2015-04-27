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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.util.MockCollector;
import org.apache.flink.streaming.util.MockSource;
import org.junit.Test;

public class SourceTest {

	@Test
	public void fromElementsTest() {
		List<Integer> expectedList = Arrays.asList(1, 2, 3);
		List<Integer> actualList = MockSource.createAndExecute(new FromElementsFunction<Integer>(1,
				2, 3));
		assertEquals(expectedList, actualList);
	}

	@Test
	public void fromCollectionTest() {
		List<Integer> expectedList = Arrays.asList(1, 2, 3);
		List<Integer> actualList = MockSource.createAndExecute(new FromElementsFunction<Integer>(
				Arrays.asList(1, 2, 3)));
		assertEquals(expectedList, actualList);
	}

	@Test
	public void socketTextStreamTest() throws Exception {
		List<String> expectedList = Arrays.asList("a", "b", "c");
		List<String> actualList = new ArrayList<String>();

		byte[] data = { 'a', '\n', 'b', '\n', 'c', '\n' };

		Socket socket = mock(Socket.class);
		when(socket.getInputStream()).thenReturn(new ByteArrayInputStream(data));
		when(socket.isClosed()).thenReturn(false);
		when(socket.isConnected()).thenReturn(true);

		new SocketTextStreamFunction("", 0, '\n', 0).streamFromSocket(new MockCollector<String>(
				actualList), socket);
		assertEquals(expectedList, actualList);
	}
}
