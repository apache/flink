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

package org.apache.flink.mesos.scheduler;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of {@link SchedulerDriver}.
 */
public class TestingSchedulerDriver implements SchedulerDriver {

	private final Supplier<Protos.Status> startSupplier;
	private final Function<Boolean, Protos.Status> stopFunction;
	private final Function<Protos.TaskStatus, Protos.Status> acknowledgeStatusUpdateFunction;
	private final TriFunction<
			Collection<Protos.OfferID>,
			Collection<Protos.Offer.Operation>,
			Protos.Filters,
			Protos.Status> acceptOffersFunction;

	private TestingSchedulerDriver(
			final Supplier<Protos.Status> startSupplier,
			final Function<Boolean, Protos.Status> stopFunction,
			final Function<Protos.TaskStatus, Protos.Status> acknowledgeStatusUpdateFunction,
			final TriFunction<
					Collection<Protos.OfferID>,
					Collection<Protos.Offer.Operation>,
					Protos.Filters,
					Protos.Status> acceptOffersFunction) {
		this.startSupplier = startSupplier;
		this.stopFunction = stopFunction;
		this.acknowledgeStatusUpdateFunction = acknowledgeStatusUpdateFunction;
		this.acceptOffersFunction = acceptOffersFunction;
	}

	@Override
	public Protos.Status start() {
		return startSupplier.get();
	}

	@Override
	public Protos.Status stop(boolean failover) {
		return stopFunction.apply(failover);
	}

	@Override
	public Protos.Status acknowledgeStatusUpdate(Protos.TaskStatus status) {
		return acknowledgeStatusUpdateFunction.apply(status);
	}

	@Override
	public Protos.Status acceptOffers(Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations, Protos.Filters filters) {
		return acceptOffersFunction.apply(offerIds, operations, filters);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for {@link TestingSchedulerDriver}.
	 */
	public static class Builder {
		private Supplier<Protos.Status> startSupplier = () -> null;
		private Function<Boolean, Protos.Status> stopFunction = (ignore) -> null;
		private Function<Protos.TaskStatus, Protos.Status> acknowledgeStatusUpdateFunction = (ignore) -> null;
		private TriFunction<
				Collection<Protos.OfferID>,
				Collection<Protos.Offer.Operation>,
				Protos.Filters,
				Protos.Status> acceptOffersFunction = (ignore1, ignore2, ignore3) -> null;

		public Builder setStartSupplier(Supplier<Protos.Status> startSupplier) {
			this.startSupplier = Preconditions.checkNotNull(startSupplier);
			return this;
		}

		public Builder setStopFunction(Function<Boolean, Protos.Status> stopFunction) {
			this.stopFunction = Preconditions.checkNotNull(stopFunction);
			return this;
		}

		public Builder setAcknowledgeStatusUpdateFunction(Function<Protos.TaskStatus, Protos.Status> acknowledgeStatusUpdateFunction) {
			this.acknowledgeStatusUpdateFunction = Preconditions.checkNotNull(acknowledgeStatusUpdateFunction);
			return this;
		}

		public Builder setAcceptOffersFunction(
				TriFunction<Collection<Protos.OfferID>, Collection<Protos.Offer.Operation>, Protos.Filters, Protos.Status> acceptOffersFunction) {
			this.acceptOffersFunction = Preconditions.checkNotNull(acceptOffersFunction);
			return this;
		}

		public TestingSchedulerDriver build() {
			return new TestingSchedulerDriver(startSupplier, stopFunction, acknowledgeStatusUpdateFunction, acceptOffersFunction);
		}
	}

	// ------------------------------------------------------------------------
	//  Unsupported
	// ------------------------------------------------------------------------

	@Override
	public Protos.Status stop() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status abort() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status join() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status run() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status requestResources(Collection<Protos.Request> requests) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status killTask(Protos.TaskID taskId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status declineOffer(Protos.OfferID offerId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status reviveOffers() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status suppressOffers() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status sendFrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Protos.Status reconcileTasks(Collection<Protos.TaskStatus> statuses) {
		throw new UnsupportedOperationException();
	}
}
