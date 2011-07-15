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

package eu.stratosphere.pact.common.recordstubs;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * The MatchStub must be extended to provide a matcher implementation which is
 * called by a Match PACT. By definition, a Match PACT has two input sets of
 * key-value pairs. It calls the match implementation once for each two pairs
 * that share the same key and come from different input sets. It resembles an
 * equality join of both inputs on their keys. For details on the Match PACT
 * read the documentation of the PACT programming model.
 * <p>
 * The MatchStub extension must be parameterized with the type of the key that is matched on.
 * <p>
 * For a match implementation, the <code>match()</code> method must be implemented.
 * 
 * @author Fabian Hueske
 * @param <K> Type of the input key.
 */
public abstract class MatchStub<K extends Key> extends SingleInputKeyStub<K>
{
	/**
	 * This method must be implemented to provide a user implementation of a matcher.
	 * It is called for each two records that share the same key and come from different inputs.
	 * 
	 * @param key The key that is shared by both records.
	 * @param value1 The record that comes from the first input.
	 * @param value2 The record that comes from the second input.
	 * @param out A collector that collects all output pairs.
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the combine task and lets the fail-over logic
	 *                   decide whether to retry the combiner execution.
	 */
	public abstract void match(K key, PactRecord value1, PactRecord value2, Collector out) throws Exception;
}
