/**
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

package org.apache.flink.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * A Hadoop OutputCollector that basically wraps a Flink OutputCollector.
 * This implies that on each call of collect() the data is actually collected.
 */
public class HadoopOutputCollector<KEYOUT extends WritableComparable, VALUEOUT extends Writable>
		implements OutputCollector<KEYOUT,VALUEOUT> {

	private Collector<Tuple2<KEYOUT,VALUEOUT>> collector;
	private Class<KEYOUT> keyoutClass;
	private Class<VALUEOUT> valueoutClass;

	public HadoopOutputCollector(Class<KEYOUT> keyoutClass, Class<VALUEOUT> valueoutClass) {
		this.keyoutClass = keyoutClass;
		this.valueoutClass = valueoutClass;
	}

	public HadoopOutputCollector() {}  //Useful when instantiating by reflection.

	public void set(Collector<Tuple2<KEYOUT,VALUEOUT>> collector) {
		this.collector = collector;
	}

	@Override
	public void collect(KEYOUT keyout, VALUEOUT valueout) throws IOException {
		validateExpectedTypes(keyout, valueout);

		final Tuple2<KEYOUT,VALUEOUT> tuple = new Tuple2<KEYOUT, VALUEOUT>(keyout, valueout);
		if (this.collector != null) {
			this.collector.collect(tuple);
		}
		else {
			throw new RuntimeException("There is no Flink Collector set to be wrapped by this" +
					" HadoopOutputCollector object. The set method must be called in advance.");
		}
	}

	/**
	 * Method to set the expected key and value output classes. Must be used if instantiating by reflection
	 * (e.g. custom serialization).
	 */
	public void setExpectedKeyValueClasses(Class<KEYOUT> keyClass, Class<VALUEOUT> valueClass) {
		this.keyoutClass = keyClass;
		this.valueoutClass = valueClass;
	}

	/**
	 * Checking if types of current key and value are compatible with the expected ones.
	 */
	private void validateExpectedTypes(KEYOUT keyout, VALUEOUT valueout) throws IOException{
		if (this.keyoutClass == null) {
			throw new IOException("Expected output key class has not been specified.");
		}
		else if (! this.keyoutClass.isInstance(keyout)) {
			final String kClassName = keyout.getClass().getCanonicalName();
			throw new IOException("Type mismatch in key: expected " + this.keyoutClass + ", received " + kClassName);
		}

		if (this.valueoutClass == null) {
			throw new IOException("Expected output value class has not been specified.");
		}
		else if (! this.valueoutClass.isInstance(valueout)) {
			final String vClassName = valueout.getClass().getCanonicalName();
			throw new IOException("Type mismatch in value: expected " + this.valueoutClass + ", received " + vClassName);
		}
	}
}