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

package org.apache.flink.yarn;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * A Yarn {@link RegisterApplicationMasterResponse} implementation for testing.
 */
public class TestingRegisterApplicationMasterResponse extends RegisterApplicationMasterResponsePBImpl {
	private static final Resource MAX_CAPABILITY = Resource.newInstance(1024 * 10000, 10000);
	private final Supplier<List<Container>> getContainersFromPreviousAttemptsSupplier;

	TestingRegisterApplicationMasterResponse(Supplier<List<Container>> getContainersFromPreviousAttemptsSupplier) {
		this.getContainersFromPreviousAttemptsSupplier = getContainersFromPreviousAttemptsSupplier;
	}

	/**
	 * The @Override annotation is intentionally removed for this method to align with the production codes' assumption
	 * that this interface may not be available for the given hadoop version.
	 */
	public List<Container> getContainersFromPreviousAttempts() {
		return getContainersFromPreviousAttemptsSupplier.get();
	}

	/**
	 * The @Override annotation is intentionally removed for this method to align with the production codes' assumption
	 * that this interface may not be available for the given hadoop version.
	 */
	@SuppressWarnings("unchecked")
	public EnumSet getSchedulerResourceTypes() {
		return EnumSet.copyOf(Collections.<Enum>emptySet());
	}

	@Override
	public Resource getMaximumResourceCapability() {
		return MAX_CAPABILITY;
	}
}
