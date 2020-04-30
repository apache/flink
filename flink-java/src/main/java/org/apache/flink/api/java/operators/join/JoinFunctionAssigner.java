/*
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

package org.apache.flink.api.java.operators.join;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.operators.JoinOperator;

/**
 * A Join transformation that needs to be finished by specifying either a
 * {@link JoinFunction} or a {@link FlatJoinFunction} before it can be used as an input
 * to other operators.
 *
 * @param <I1> The type of the first input DataSet of the Join transformation.
 * @param <I2> The type of the second input DataSet of the Join transformation.
 */
@Public
public interface JoinFunctionAssigner<I1, I2> {

	<R> JoinOperator<I1, I2, R> with(JoinFunction<I1, I2, R> joinFunction);

	<R> JoinOperator<I1, I2, R> with(FlatJoinFunction<I1, I2, R> joinFunction);

}
