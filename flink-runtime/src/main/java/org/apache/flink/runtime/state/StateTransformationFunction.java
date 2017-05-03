/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

/**
 * Interface for a binary function that is used for push-down of state transformation into state backends. The
 * function takes as inputs the old state and an element. From those inputs, the function computes the new state.
 *
 * @param <S> type of the previous state that is the bases for the computation of the new state.
 * @param <T> type of the element value that is used to compute the change of state.
 */
@Internal
public interface StateTransformationFunction<S, T> {

	/**
	 * Binary function that applies a given value to the given old state to compute the new state.
	 *
	 * @param previousState the previous state that is the basis for the transformation.
	 * @param value         the value that the implementation applies to the old state to obtain the new state.
	 * @return the new state, computed by applying the given value on the given old state.
	 * @throws Exception if something goes wrong in applying the transformation function.
	 */
	S apply(S previousState, T value) throws Exception;
}