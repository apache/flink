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

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A {@link DataView} is a collection type that can be used in the accumulator of an
 * {@link org.apache.flink.table.functions.AggregateFunction}.
 *
 * <p>Depending on the context in which the {@code AggregateFunction} is
 * used, a {@link DataView} can be backed by a Java heap collection or a state backend.
 */
@PublicEvolving
public interface DataView extends Serializable {

	/**
	 * Clears the {@link DataView} and removes all data.
	 */
	void clear();
}
