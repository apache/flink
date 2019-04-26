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
import org.apache.flink.table.api.TableEnvironment;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;

/**
 * Descriptor for specifying a table source and/or sink in a streaming environment.
 */
@PublicEvolving
public class StreamTableDescriptor
	extends ConnectTableDescriptor<StreamTableDescriptor>
	implements StreamableDescriptor<StreamTableDescriptor> {

	private Optional<String> updateMode = Optional.empty();

	public StreamTableDescriptor(TableEnvironment tableEnv, ConnectorDescriptor connectorDescriptor) {
		super(tableEnv, connectorDescriptor);
	}

	/**
	 * Declares how to perform the conversion between a dynamic table and an external connector.
	 *
	 * <p>In append mode, a dynamic table and an external connector only exchange INSERT messages.
	 *
	 * @see #inRetractMode()
	 * @see #inUpsertMode()
	 */
	@Override
	public StreamTableDescriptor inAppendMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_APPEND);
		return this;
	}

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
	@Override
	public StreamTableDescriptor inRetractMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_RETRACT);
		return this;
	}

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
	@Override
	public StreamTableDescriptor inUpsertMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_UPSERT);
		return this;
	}

	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();

		properties.putProperties(super.toProperties());
		updateMode.ifPresent(mode -> properties.putString(UPDATE_MODE, mode));

		return properties.asMap();
	}
}
