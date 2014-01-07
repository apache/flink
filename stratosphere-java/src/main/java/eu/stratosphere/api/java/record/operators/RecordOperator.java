/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.java.record.operators;

import eu.stratosphere.types.Key;

/**
 * Interface marking contract classes to be referring to the {@link Record} data model.
 */
public interface RecordOperator {
	
	/**
	 * Gets the key classes that this record works on.
	 * 
	 * @return The key classes that this record works on.
	 */
	Class<? extends Key>[] getKeyClasses();
}
