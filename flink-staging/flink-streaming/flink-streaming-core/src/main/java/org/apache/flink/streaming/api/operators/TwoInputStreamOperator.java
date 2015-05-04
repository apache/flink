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

package org.apache.flink.streaming.api.operators;

/**
 * Interface for stream operators with two inputs. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 * 
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 */
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

	public void processElement1(IN1 element) throws Exception;

	public void processElement2(IN2 element) throws Exception;
}
