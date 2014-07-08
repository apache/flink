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

package eu.stratosphere.hadoopcompatibility.mapred.record.datatypes;

import java.io.Serializable;

import eu.stratosphere.types.Record;

/**
 * An interface describing a class that is able to
 * convert Stratosphere's Record into Hadoop types model.
 *
 * The converter must be Serializable.
 *
 * Stratosphere provides a DefaultStratosphereTypeConverter. Custom implementations should
 * chain the type converters.
 */
public interface StratosphereTypeConverter<K,V> extends Serializable {

	/**
	 * Convert a Stratosphere type to a Hadoop type.
	 */
	public K convertKey(Record stratosphereRecord);

	public V convertValue(Record stratosphereRecord);
}
