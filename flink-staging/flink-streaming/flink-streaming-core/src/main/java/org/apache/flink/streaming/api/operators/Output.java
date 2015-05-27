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

import org.apache.flink.util.Collector;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} is supplied with an object
 * of this interface that can be used to emit elements and other messages, such as barriers
 * and low watermarks, from an operator.
 *
 * @param <T> The type of the elments that can be emitted.
 */
public interface Output<T> extends Collector<T> {
	// NOTE: This does not yet have methods for barriers/low watermarks, this needs to be
	// extended when this functionality arrives.
}
