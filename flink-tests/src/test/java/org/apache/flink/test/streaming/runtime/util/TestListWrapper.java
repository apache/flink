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

package org.apache.flink.test.streaming.runtime.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Catalog for lists stored by {@link TestListResultSink}.
 */
public class TestListWrapper {

	private static TestListWrapper instance;

	@SuppressWarnings("rawtypes")
	private List<List<? extends Comparable>> lists;

	@SuppressWarnings("rawtypes")
	private TestListWrapper() {
		lists = Collections.synchronizedList(new ArrayList<List<? extends Comparable>>());
	}

	public static TestListWrapper getInstance() {
		if (instance == null) {
			instance = new TestListWrapper();
		}
		return instance;
	}

	/**
	 * Creates and stores a list, returns with the id.
	 *
	 * @return The ID of the list.
	 */
	@SuppressWarnings("rawtypes")
	public int createList() {
		lists.add(new ArrayList<Comparable>());
		return lists.size() - 1;
	}

	public List<?> getList(int listId) {
		@SuppressWarnings("rawtypes")
		List<? extends Comparable> list = lists.get(listId);
		if (list == null) {
			throw new RuntimeException("No such list.");
		}

		return list;
	}

}
