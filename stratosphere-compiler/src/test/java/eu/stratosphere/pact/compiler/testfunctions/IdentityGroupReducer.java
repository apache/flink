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

package eu.stratosphere.pact.compiler.testfunctions;

import java.util.Iterator;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction.Combinable;
import eu.stratosphere.util.Collector;


@Combinable
public class IdentityGroupReducer<T> extends GroupReduceFunction<T, T> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterator<T> values, Collector<T> out) {
		while (values.hasNext()) {
			out.collect(values.next());
		}
	}
}
