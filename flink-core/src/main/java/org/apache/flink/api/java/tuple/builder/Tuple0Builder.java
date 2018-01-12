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
import org.apache.flink.api.java.tuple.Tuple0;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Tuple0}.
 */
@Public
public class Tuple0Builder {

	private List<Tuple0> tuples = new ArrayList<Tuple0>();

	public Tuple0Builder add() {
		tuples.add(Tuple0.INSTANCE);
		return this;
	}

	public Tuple0[] build() {
		return tuples.toArray(new Tuple0[tuples.size()]);
	}

}
