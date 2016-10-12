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

package org.apache.flink.streaming.api.operators.async;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link SimpleLinkedList}. These test that:
 *
 * <ul>
 *     <li>Add a new item into list</li>
 *     <li>Remove items from list</li>
 * </ul>
 */

public class SimpleLinkedListTest {
	@Test
	public void test() {
		SimpleLinkedList<Integer> list = new SimpleLinkedList<>();
		SimpleLinkedList.Node node1 = list.add(0);
		SimpleLinkedList.Node node2 = list.add(1);
		list.add(2);

		Assert.assertTrue(list.get(0) == 0);
		Assert.assertTrue(list.get(2) == 2);

		Assert.assertTrue(list.size == 3);

		Assert.assertEquals(list.node(0), node1);
		Assert.assertEquals(list.node(1), node2);

		list.remove(0);
		Assert.assertTrue(list.size == 2);
		Assert.assertTrue(list.get(0) == 1);

		list.remove(node2);
		Assert.assertTrue(list.size == 1);
		Assert.assertTrue(list.get(0) == 2);
	}
}
