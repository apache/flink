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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This file belongs to the PojoComparatorTest test
 *
 */
public class PojoContainingTuple {
	public int someInt;
	public String someString = "abc";
	public Tuple2<Long, Long> theTuple;
	public PojoContainingTuple() {}
	public PojoContainingTuple(int i, long l1, long l2) {
		someInt = i;
		theTuple = new Tuple2<Long, Long>(l1, l2);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PojoContainingTuple) {
			PojoContainingTuple other = (PojoContainingTuple) obj;
			return someInt == other.someInt && theTuple.equals(other.theTuple);
		}
		return false;
	}
}