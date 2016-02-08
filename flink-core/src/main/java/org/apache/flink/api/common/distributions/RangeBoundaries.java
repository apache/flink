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
package org.apache.flink.api.common.distributions;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * RangeBoundaries is used to split the records into multiple ranges.
 *
 * @param <T> The boundary type.
 */
@PublicEvolving
public interface RangeBoundaries<T> extends Serializable {

	/**
	 * Get the range index of record.
	 *
	 * @param record     The input record.
	 * @return The range index.
	 */
	int getRangeIndex(T record);
}
