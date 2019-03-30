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

package org.apache.flink.table.generated;

import org.apache.flink.table.dataformat.BaseRow;

import java.io.Serializable;

/**
 * Record equaliser for BaseRow which can compare two BaseRows and returns whether they are equal.
 */
public interface RecordEqualiser extends Serializable {

	/**
	 * Returns {@code true} if the rows are equal to each other
	 * and {@code false} otherwise.
	 */
	boolean equals(BaseRow row1, BaseRow row2);

	/**
	 * Returns {@code true} if the rows are equal to each other without header compare
	 * and {@code false} otherwise.
	 */
	boolean equalsWithoutHeader(BaseRow row1, BaseRow row2);
}
