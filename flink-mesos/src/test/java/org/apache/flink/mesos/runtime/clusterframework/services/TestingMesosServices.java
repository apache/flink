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

package org.apache.flink.mesos.runtime.clusterframework.services;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManagerActorFactory;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;

import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of {@link MesosServices}.
 */
public class TestingMesosServices implements MesosServices {

	private final Function<Configuration, MesosWorkerStore> createMesosWorkerStoreFunction;
	private final Supplier<MesosResourceManagerActorFactory> getMesosResourceManagerActorFactorySupplier;
	private final Supplier<MesosArtifactServer> getArtifactServerSupplier;
	private final TriFunction<MesosConfiguration, Scheduler, Boolean, SchedulerDriver> createMesosSchedulerDriverFunction;
	private final Consumer<Boolean> closeConsumer;

	private TestingMesosServices(
			final Function<Configuration, MesosWorkerStore> createMesosWorkerStoreFunction,
			final Supplier<MesosResourceManagerActorFactory> getMesosResourceManagerActorFactorySupplier,
			final Supplier<MesosArtifactServer> getArtifactServerSupplier,
			final TriFunction<MesosConfiguration, Scheduler, Boolean, SchedulerDriver> createMesosSchedulerDriverFunction,
			final Consumer<Boolean> closeConsumer) {
		this.createMesosWorkerStoreFunction = createMesosWorkerStoreFunction;
		this.getMesosResourceManagerActorFactorySupplier = getMesosResourceManagerActorFactorySupplier;
		this.getArtifactServerSupplier = getArtifactServerSupplier;
		this.createMesosSchedulerDriverFunction = createMesosSchedulerDriverFunction;
		this.closeConsumer = closeConsumer;
	}

	@Override
	public MesosWorkerStore createMesosWorkerStore(Configuration configuration) {
		return createMesosWorkerStoreFunction.apply(configuration);
	}

	@Override
	public MesosResourceManagerActorFactory createMesosResourceManagerActorFactory() {
		return getMesosResourceManagerActorFactorySupplier.get();
	}

	@Override
	public MesosArtifactServer getArtifactServer() {
		return getArtifactServerSupplier.get();
	}

	@Override
	public SchedulerDriver createMesosSchedulerDriver(MesosConfiguration mesosConfig, Scheduler scheduler, boolean implicitAcknowledgements) {
		return createMesosSchedulerDriverFunction.apply(mesosConfig, scheduler, implicitAcknowledgements);
	}

	@Override
	public void close(boolean cleanup) {
		closeConsumer.accept(cleanup);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for {@link TestingMesosServices}.
	 */
	public static class Builder {
		private Function<Configuration, MesosWorkerStore> createMesosWorkerStoreFunction = (ignore) -> null;
		private Supplier<MesosResourceManagerActorFactory> getMesosResourceManagerActorFactorySupplier = () -> null;
		private Supplier<MesosArtifactServer> getArtifactServerSupplier = () -> null;
		private TriFunction<MesosConfiguration, Scheduler, Boolean, SchedulerDriver> createMesosSchedulerDriverFunction =
				(ignore1, ignore2, ignore3) -> null;
		private Consumer<Boolean> closeConsumer = (ignore) -> {};

		private Builder() {}

		public Builder setCreateMesosWorkerStoreFunction(Function<Configuration, MesosWorkerStore> createMesosWorkerStoreFunction) {
			this.createMesosWorkerStoreFunction = Preconditions.checkNotNull(createMesosWorkerStoreFunction);
			return this;
		}

		public Builder setGetMesosResourceManagerActorFactorySupplier(Supplier<MesosResourceManagerActorFactory> getMesosResourceManagerActorFactorySupplier) {
			this.getMesosResourceManagerActorFactorySupplier = Preconditions.checkNotNull(getMesosResourceManagerActorFactorySupplier);
			return this;
		}

		public Builder setGetArtifactServerSupplier(Supplier<MesosArtifactServer> getArtifactServerSupplier) {
			this.getArtifactServerSupplier = Preconditions.checkNotNull(getArtifactServerSupplier);
			return this;
		}

		public Builder setCreateMesosSchedulerDriverFunction(
				TriFunction<MesosConfiguration, Scheduler, Boolean, SchedulerDriver> createMesosSchedulerDriverFunction) {
			this.createMesosSchedulerDriverFunction = Preconditions.checkNotNull(createMesosSchedulerDriverFunction);
			return this;
		}

		public Builder setCloseConsumer(Consumer<Boolean> closeConsumer) {
			this.closeConsumer = Preconditions.checkNotNull(closeConsumer);
			return this;
		}

		public TestingMesosServices build() {
			return new TestingMesosServices(
					createMesosWorkerStoreFunction,
					getMesosResourceManagerActorFactorySupplier,
					getArtifactServerSupplier,
					createMesosSchedulerDriverFunction,
					closeConsumer);
		}
	}
}
