/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api.collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;

/**
 * Class for defining an OutputSelector for a {@link SplitDataStream} using the
 * {@link SingleOutputStreamOperator#split} call. Every output object of a
 * {@link SplitDataStream} will run through this operator to select outputs.
 * 
 * @param <OUT>
 *            Type parameter of the split values.
 */
public abstract class OutputSelector<OUT> implements Serializable {
	private static final long serialVersionUID = 1L;

	private Collection<String> outputs;

	public OutputSelector() {
		outputs = new ArrayList<String>();
	}

	Collection<String> getOutputs(OUT outputObject) {
		outputs.clear();
		select(outputObject, outputs);
		return outputs;
	}

	/**
	 * Method for selecting output names for the emitted objects when using the
	 * {@link SingleOutputStreamOperator#split} method. The values will be
	 * emitted only to output names which are added to the outputs collection.
	 * The outputs collection is cleared automatically after each select call.
	 * 
	 * @param value
	 *            Output object for which the output selection should be made.
	 * @param outputs
	 *            Selected output names should be added to this collection.
	 */
	public abstract void select(OUT value, Collection<String> outputs);
}