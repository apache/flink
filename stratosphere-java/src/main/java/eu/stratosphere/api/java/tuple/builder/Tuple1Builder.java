/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------


package eu.stratosphere.api.java.tuple.builder;

import java.util.List;
import java.util.LinkedList;
import eu.stratosphere.api.java.tuple.Tuple1;

public class Tuple1Builder<T0> {

	private List<Tuple1<T0>> tuples = new LinkedList<Tuple1<T0>>();

	public Tuple1Builder<T0> add(T0 value0){
		tuples.add(new Tuple1<T0>(value0));
		return this;
	}

	@SuppressWarnings("unchecked")
	public Tuple1<T0>[] build(){
		return tuples.toArray(new Tuple1[tuples.size()]);
	}
}
