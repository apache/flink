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

package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Lists all kinds of changes that a row can describe in a changelog.
 */
@PublicEvolving
public enum RowKind {

	/**
	 * Insertion operation.
	 */
	INSERT,

	/**
	 * Update operation with the previous content of the updated row.
	 *
	 * <p>This kind SHOULD occur together with {@link #UPDATE_AFTER} for modelling an update that needs
	 * to retract the previous row first. It is useful in cases of a non-idempotent update, i.e., an
	 * update of a row that is not uniquely identifiable by a key.
	 */
	UPDATE_BEFORE,

	/**
	 * Update operation with new content of the updated row.
	 *
	 * <p>This kind CAN occur together with {@link #UPDATE_BEFORE} for modelling an update that
	 * needs to retract the previous row first. OR it describes an idempotent update, i.e., an update
	 * of a row that is uniquely identifiable by a key.
	 */
	UPDATE_AFTER,

	/**
	 * Deletion operation.
	 */
	DELETE
}
