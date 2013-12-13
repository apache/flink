/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
 * A simple factory to instantiate record deserializer objects. Since a deserializer might be stateful, the system
 * must be able to instantiate an arbitrary number of them, equal to the number of data channels.
 * 
 * If the created deserializers are in fact not stateful, the factory should return a shared object.
 */
public interface RecordDeserializerFactory<T>
{
	/**
	 * Creates a new instance of the deserializer. The returned instance may not share any state with
	 * any previously returned instance.
	 * 
	 * @return An instance of the deserializer.
	 */
	RecordDeserializer<T> createDeserializer();
}
