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

package eu.stratosphere.nephele.services.iomanager;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Erik Nijkamp
 */
public interface Serializer<T> {
	/**
	 * <p>
	 * Prepare the serializer for writing.
	 * </p>
	 */
	void open(DataOutput out) throws IOException;

	/**
	 * <p>
	 * Serialize <code>t</code> to the underlying output stream.
	 * </p>
	 */
	void serialize(T t) throws IOException;

	/**
	 * <p>
	 * Clear up any resources.
	 * </p>
	 */
	void close() throws IOException;
}
