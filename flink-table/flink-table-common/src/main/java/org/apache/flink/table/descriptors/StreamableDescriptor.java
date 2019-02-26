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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A trait for descriptors that allow to convert between a dynamic table and an external connector.
 */
@PublicEvolving
public interface StreamableDescriptor<D extends StreamableDescriptor<D>> extends Descriptor {

	/**
	 * Declares how to perform the conversion between a dynamic table and an external connector.
	 *
	 * <p>In append mode, a dynamic table and an external connector only exchange INSERT messages.
	 *
	 * @see #inRetractMode()
	 * @see #inUpsertMode()
	 */
	D inAppendMode();

	/**
	 * Declares how to perform the conversion between a dynamic table and an external connector.
	 *
	 * <p>In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages.
	 *
	 * <p>An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
	 * UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
	 * the updating (new) row.
	 *
	 * <p>In this mode, a key must not be defined as opposed to upsert mode. However, every update
	 * consists of two messages which is less efficient.
	 *
	 * @see #inAppendMode()
	 * @see #inUpsertMode()
	 */
	D inRetractMode();

	/**
	 * Declares how to perform the conversion between a dynamic table and an external connector.
	 *
	 * <p>In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages.
	 *
	 * <p>This mode requires a (possibly composite) unique key by which updates can be propagated. The
	 * external connector needs to be aware of the unique key attribute in order to apply messages
	 * correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
	 * DELETE messages.
	 *
	 * <p>The main difference to a retract stream is that UPDATE changes are encoded with a single
	 * message and are therefore more efficient.
	 *
	 * @see #inAppendMode()
	 * @see #inRetractMode()
	 */
	D inUpsertMode();
}
