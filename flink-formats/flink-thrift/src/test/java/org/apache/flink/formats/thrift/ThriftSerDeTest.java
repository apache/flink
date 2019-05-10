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

package org.apache.flink.formats.thrift;

import org.apache.flink.formats.thrift.generated.Diagnostics;
import org.apache.flink.formats.thrift.generated.Event;
import org.apache.flink.formats.thrift.generated.EventBinaryData;
import org.apache.flink.formats.thrift.generated.StructWithBinaryField;
import org.apache.flink.formats.thrift.generated.UserAction;
import org.apache.flink.formats.thrift.generated.nested.LLColl;
import org.apache.flink.formats.thrift.generated.nested.LMColl;
import org.apache.flink.formats.thrift.generated.nested.LSColl;
import org.apache.flink.formats.thrift.generated.nested.MLColl;
import org.apache.flink.formats.thrift.generated.nested.MMColl;
import org.apache.flink.formats.thrift.generated.nested.MSColl;
import org.apache.flink.formats.thrift.generated.nested.SLColl;
import org.apache.flink.formats.thrift.generated.nested.SMColl;
import org.apache.flink.formats.thrift.generated.nested.SSColl;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ThriftSerializer.
 */
public class ThriftSerDeTest {

	private static Event generateTestEventData() {
		Event event = new Event();
		UserAction userAction = new UserAction();
		userAction.setTimestamp(System.currentTimeMillis());
		userAction.setReferralUrl("http://www.pinterest.com/");
		userAction.setUrl("http://www.youtube.com");

		event.userActions = new HashMap<>();
		event.userActions.put("action1", userAction);

		Diagnostics diagnostics = new Diagnostics();
		diagnostics.setHostname("testhost001");
		diagnostics.setIpaddress("123.234.99.88");

		event.setDiagnostics(diagnostics);
		return event;
	}

	@Test
	public void testThriftSerializer() throws IOException {
		Event event = generateTestEventData();
		assertTrue(TestUtils.isSerDeConsistentFor(event));
	}

	@Test
	public void testThriftBinarySerDe() throws IOException {
		byte[] binaryData = "0123456789".getBytes();
		EventBinaryData eventBinaryData = new EventBinaryData();
		eventBinaryData.setTimestamp(System.currentTimeMillis());
		eventBinaryData.setUserId(1000);
		eventBinaryData.setStringData("event's string data");
		eventBinaryData.setBinaryData(binaryData);
		assertTrue(TestUtils.isSerDeConsistentFor(eventBinaryData));
	}

	@Test
	public void testThriftBinarySerDe2() throws IOException {
		StructWithBinaryField message = new StructWithBinaryField();
		message.setSegmentIds("1234567".getBytes());
		assertTrue(TestUtils.isSerDeConsistentFor(message));
	}

	@Test
	public void testNestedCollections() throws IOException {
		List<String> list1OfStrings = Arrays.asList("item11", "item12");
		List<String> list2OfStrings = Arrays.asList("item21");
		List<String> list3OfStrings = Arrays.asList("item31", "item32", "item33");

		Map<Integer, String> map1OfIntegerToString = new HashMap<>();
		map1OfIntegerToString.put(0, "value10");
		map1OfIntegerToString.put(1, "value11");
		map1OfIntegerToString.put(2, "value12");
		Map<Integer, String> map2OfIntegerToString = new HashMap<>();
		map1OfIntegerToString.put(0, "value20");

		Set<String> set1OfString = new HashSet<>();
		set1OfString.add("member11");
		set1OfString.add("member12");
		set1OfString.add("member13");
		Set<String> set2OfString = new HashSet<>();
		set2OfString.add("member21");
		set2OfString.add("member22");
		Set<String> set3OfString = new HashSet<>();
		set3OfString.add("member31");

		List<List<String>> listOfLists = new ArrayList<>(Arrays.asList(list1OfStrings, list2OfStrings, list3OfStrings));
		List<Map<Integer, String>> listOfMaps = new ArrayList<>(
			Arrays.asList(map1OfIntegerToString, map2OfIntegerToString));
		List<Set<String>> listOfSets = new ArrayList<>(Arrays.asList(set1OfString, set2OfString, set3OfString));

		Map<Integer, List<String>> mapOfIntegerToList = new HashMap<>();
		mapOfIntegerToList.put(1, list1OfStrings);
		mapOfIntegerToList.put(2, list2OfStrings);
		mapOfIntegerToList.put(3, list3OfStrings);
		Map<Integer, Map<Integer, String>> mapOfIntegerToMap = new HashMap<>();
		mapOfIntegerToMap.put(10, map1OfIntegerToString);
		Map<Integer, Set<String>> mapOfIntegerToSet = new HashMap<>();
		mapOfIntegerToSet.put(100, set1OfString);
		mapOfIntegerToSet.put(200, set2OfString);

		Set<List<String>> setOfLists = new HashSet<>();
		setOfLists.add(list1OfStrings);
		setOfLists.add(list2OfStrings);
		setOfLists.add(list3OfStrings);
		Set<Map<Integer, String>> setOfMaps = new HashSet<>();
		setOfMaps.add(map1OfIntegerToString);
		setOfMaps.add(map2OfIntegerToString);
		Set<Set<String>> setOfSets = new HashSet<>();
		setOfSets.add(set1OfString);

		assertTrue(TestUtils.isSerDeConsistentFor(new LLColl(listOfLists)));
		assertTrue(TestUtils.isSerDeConsistentFor(new LMColl(listOfMaps)));
		assertTrue(TestUtils.isSerDeConsistentFor(new LSColl(listOfSets)));
		assertTrue(TestUtils.isSerDeConsistentFor(new MLColl(mapOfIntegerToList)));
		assertTrue(TestUtils.isSerDeConsistentFor(new MMColl(mapOfIntegerToMap)));
		assertTrue(TestUtils.isSerDeConsistentFor(new MSColl(mapOfIntegerToSet)));
		assertTrue(TestUtils.isSerDeConsistentFor(new SLColl(setOfLists)));
		assertTrue(TestUtils.isSerDeConsistentFor(new SMColl(setOfMaps)));
		assertTrue(TestUtils.isSerDeConsistentFor(new SSColl(setOfSets)));
	}
}
