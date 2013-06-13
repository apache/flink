/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.generic.stub;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;

public interface GenericCoGrouper<V1, V2, O> extends Stub {
	
	public abstract void coGroup(Iterator<V1> records1, Iterator<V2> records2, Collector<O> out) throws Exception;
	
	public void combineFirst(Iterator<V1> records, Collector<V1> out) throws Exception;
	
	public void combineSecond(Iterator<V2> records, Collector<V2> out) throws Exception;
}
