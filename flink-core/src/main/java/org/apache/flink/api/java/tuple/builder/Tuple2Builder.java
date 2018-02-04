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

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package org.apache.flink.api.java.tuple.builder;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Tuple2}.
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 */
@Public
public class Tuple2Builder<T0, T1> {

	private List<Tuple2<T0, T1>> tuples = new ArrayList<>();

	public Tuple2Builder<T0, T1> add(T0 value0, T1 value1){
		tuples.add(new Tuple2<>(value0, value1));
		return this;
	}

	@SuppressWarnings("unchecked")
	public Tuple2<T0, T1>[] build(){
		return tuples.toArray(new Tuple2[tuples.size()]);
	}
}
