/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io;

/**
 * This interface defines a factory which is able to instantiate new record objects of the type T. A record factory is
 * required when the user code chooses to use immutable records over mutable ones.
 * 
 * @author warneke
 * @param <T>
 *        the type of records instantiated by this factory.
 */
public interface RecordFactory<T> {

	/**
	 * Creates a new record of type T.
	 * 
	 * @return a new record of type T
	 */
	T createRecord();
}
