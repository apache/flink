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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * The {@link JobMaster} fencing token.
 */
public class JobMasterId extends AbstractID {

	private static final long serialVersionUID = -933276753644003754L;

	/**
	 * Creates a JobMasterId that takes the bits from the given UUID.
	 */
	public JobMasterId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	/**
	 * Generates a new random JobMasterId.
	 */
	private JobMasterId() {
		super();
	}

	/**
	 * Creates a UUID with the bits from this JobMasterId.
	 */
	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	/**
	 * Generates a new random JobMasterId.
	 */
	public static JobMasterId generate() {
		return new JobMasterId();
	}

	/**
	 * If the given uuid is null, this returns null, otherwise a JobMasterId that
	 * corresponds to the UUID, via {@link #JobMasterId(UUID)}.
	 */
	public static JobMasterId fromUuidOrNull(@Nullable UUID uuid) {
		return  uuid == null ? null : new JobMasterId(uuid);
	}
}
