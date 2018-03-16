/*
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
 */

package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.io.Serializable;

/**
 * Interface for defining an OutputSelector for a {@link SplitStream} using
 * the {@link SingleOutputStreamOperator#split} call. Every output object of a
 * {@link SplitStream} will run through this operator to select outputs.
 *
 * @param <OUT>
 *            Type parameter of the split values.
 */
@PublicEvolving
public interface OutputSelector<OUT> extends Serializable {
	/**
	 * Method for selecting output names for the emitted objects when using the
	 * {@link SingleOutputStreamOperator#split} method. The values will be
	 * emitted only to output names which are contained in the returned
	 * iterable.
	 *
	 * @param value
	 *            Output object for which the output selection should be made.
	 */
	Iterable<String> select(OUT value);
}
