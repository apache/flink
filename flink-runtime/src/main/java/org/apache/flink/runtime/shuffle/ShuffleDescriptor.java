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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Interface for shuffle deployment descriptor of result partition resource.
 *
 * <p>The descriptor is used for the deployment of the partition producer/consumer and their data exchange
 */
public interface ShuffleDescriptor extends Serializable {

	ResultPartitionID getResultPartitionID();

	/**
	 * Returns the location of the producing task executor if the partition occupies local resources there.
	 *
	 * <p>Indicates that this partition occupies local resources in the producing task executor. Such partition requires
	 * that the task executor is running and being connected to be able to consume the produced data. This is mostly
	 * relevant for the batch jobs and blocking result partitions which can outlive the producer lifetime and
	 * be released externally.
	 * {@link ShuffleEnvironment#releasePartitionsLocally(Collection)} can be used to release such kind of partitions locally.
	 *
	 * @return the resource id of the producing task executor if the partition occupies local resources there
	 */
	Optional<ResourceID> storesLocalResourcesOn();
}
