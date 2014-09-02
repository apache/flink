/**
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


// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------


package org.apache.flink.api.java.tuple.builder;

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple8;

public class Tuple8Builder<T0, T1, T2, T3, T4, T5, T6, T7> {

	private List<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> tuples = new LinkedList<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>();

	public Tuple8Builder<T0, T1, T2, T3, T4, T5, T6, T7> add(T0 value0, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7){
		tuples.add(new Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>(value0, value1, value2, value3, value4, value5, value6, value7));
		return this;
	}

	@SuppressWarnings("unchecked")
	public Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>[] build(){
		return tuples.toArray(new Tuple8[tuples.size()]);
	}
}
