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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Actor messages for {@link InternalKvState} lookup and registration.
 */
public interface KvStateMessage extends Serializable {

	// ------------------------------------------------------------------------
	// Lookup
	// ------------------------------------------------------------------------

	/**
	 * Actor message for looking up {@link KvStateLocation}.
	 */
	class LookupKvStateLocation implements KvStateMessage {

		private static final long serialVersionUID = 1L;

		/** JobID the KvState instance belongs to. */
		private final JobID jobId;

		/** Name under which the KvState has been registered. */
		private final String registrationName;

		/**
		 * Requests a {@link KvStateLocation} for the specified JobID and
		 * {@link InternalKvState} registration name.
		 *
		 * @param jobId            JobID the KvState instance belongs to
		 * @param registrationName Name under which the KvState has been registered
		 */
		public LookupKvStateLocation(JobID jobId, String registrationName) {
			this.jobId = Preconditions.checkNotNull(jobId, "JobID");
			this.registrationName = Preconditions.checkNotNull(registrationName, "Name");
		}

		/**
		 * Returns the JobID the KvState instance belongs to.
		 *
		 * @return JobID the KvState instance belongs to
		 */
		public JobID getJobId() {
			return jobId;
		}

		/**
		 * Returns the name under which the KvState has been registered.
		 *
		 * @return Name under which the KvState has been registered
		 */
		public String getRegistrationName() {
			return registrationName;
		}

		@Override
		public String toString() {
			return "LookupKvStateLocation{" +
					"jobId=" + jobId +
					", registrationName='" + registrationName + '\'' +
					'}';
		}
	}

	// ------------------------------------------------------------------------
	// Registration
	// ------------------------------------------------------------------------

	/**
	 * Actor message for notification of {@code KvState} registration.
	 */
	class NotifyKvStateRegistered implements KvStateMessage {

		private static final long serialVersionUID = 1L;

		/** JobID the KvState instance belongs to. */
		private final JobID jobId;

		/** JobVertexID the KvState instance belongs to. */
		private final JobVertexID jobVertexId;

		/** Key group range the KvState instance belongs to. */
		private final KeyGroupRange keyGroupRange;

		/** Name under which the KvState has been registered. */
		private final String registrationName;

		/** ID of the registered KvState instance. */
		private final KvStateID kvStateId;

		/** Server address where to find the KvState instance. */
		private final InetSocketAddress kvStateServerAddress;

		/**
		 * Notifies the JobManager about a registered {@link InternalKvState} instance.
		 *
		 * @param jobId                JobID the KvState instance belongs to
		 * @param jobVertexId          JobVertexID the KvState instance belongs to
		 * @param keyGroupRange        Key group range the KvState instance belongs to
		 * @param registrationName     Name under which the KvState has been registered
		 * @param kvStateId            ID of the registered KvState instance
		 * @param kvStateServerAddress Server address where to find the KvState instance
		 */
		public NotifyKvStateRegistered(
				JobID jobId,
				JobVertexID jobVertexId,
				KeyGroupRange keyGroupRange,
				String registrationName,
				KvStateID kvStateId,
				InetSocketAddress kvStateServerAddress) {

			this.jobId = Preconditions.checkNotNull(jobId, "JobID");
			this.jobVertexId = Preconditions.checkNotNull(jobVertexId, "JobVertexID");
			Preconditions.checkArgument(keyGroupRange != KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
			this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
			this.registrationName = Preconditions.checkNotNull(registrationName, "Registration name");
			this.kvStateId = Preconditions.checkNotNull(kvStateId, "KvStateID");
			this.kvStateServerAddress = Preconditions.checkNotNull(kvStateServerAddress, "ServerAddress");
		}

		/**
		 * Returns the JobID the KvState instance belongs to.
		 *
		 * @return JobID the KvState instance belongs to
		 */
		public JobID getJobId() {
			return jobId;
		}

		/**
		 * Returns the JobVertexID the KvState instance belongs to.
		 *
		 * @return JobVertexID the KvState instance belongs to
		 */
		public JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		/**
		 * Returns the key group index the KvState instance belongs to.
		 *
		 * @return Key group index the KvState instance belongs to
		 */
		public KeyGroupRange getKeyGroupRange() {
			return keyGroupRange;
		}

		/**
		 * Returns the name under which the KvState has been registered.
		 *
		 * @return Name under which the KvState has been registered
		 */
		public String getRegistrationName() {
			return registrationName;
		}

		/**
		 * Returns the ID of the registered KvState instance.
		 *
		 * @return ID of the registered KvState instance
		 */
		public KvStateID getKvStateId() {
			return kvStateId;
		}

		/**
		 * Returns the server address where to find the KvState instance.
		 *
		 * @return Server address where to find the KvState instance
		 */
		public InetSocketAddress getKvStateServerAddress() {
			return kvStateServerAddress;
		}

		@Override
		public String toString() {
			return "NotifyKvStateRegistered{" +
					"jobId=" + jobId +
					", jobVertexId=" + jobVertexId +
					", keyGroupRange=" + keyGroupRange +
					", registrationName='" + registrationName + '\'' +
					", kvStateId=" + kvStateId +
					", kvStateServerAddress=" + kvStateServerAddress +
					'}';
		}
	}

	/**
	 * Actor message for notification of {@code KvState} unregistration.
	 */
	class NotifyKvStateUnregistered implements KvStateMessage {

		private static final long serialVersionUID = 1L;

		/** JobID the KvState instance belongs to. */
		private final JobID jobId;

		/** JobVertexID the KvState instance belongs to. */
		private final JobVertexID jobVertexId;

		/** Key group index the KvState instance belongs to. */
		private final KeyGroupRange keyGroupRange;

		/** Name under which the KvState has been registered. */
		private final String registrationName;

		/**
		 * Notifies the JobManager about an unregistered {@link InternalKvState} instance.
		 *
		 * @param jobId                JobID the KvState instance belongs to
		 * @param jobVertexId          JobVertexID the KvState instance belongs to
		 * @param keyGroupRange        Key group range the KvState instance belongs to
		 * @param registrationName     Name under which the KvState has been registered
		 */
		public NotifyKvStateUnregistered(
				JobID jobId,
				JobVertexID jobVertexId,
				KeyGroupRange keyGroupRange,
				String registrationName) {

			this.jobId = Preconditions.checkNotNull(jobId, "JobID");
			this.jobVertexId = Preconditions.checkNotNull(jobVertexId, "JobVertexID");
			Preconditions.checkArgument(keyGroupRange != KeyGroupRange.EMPTY_KEY_GROUP_RANGE);
			this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
			this.registrationName = Preconditions.checkNotNull(registrationName, "Registration name");
		}

		/**
		 * Returns the JobID the KvState instance belongs to.
		 *
		 * @return JobID the KvState instance belongs to
		 */
		public JobID getJobId() {
			return jobId;
		}

		/**
		 * Returns the JobVertexID the KvState instance belongs to.
		 *
		 * @return JobVertexID the KvState instance belongs to
		 */
		public JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		/**
		 * Returns the key group index the KvState instance belongs to.
		 *
		 * @return Key group index the KvState instance belongs to
		 */
		public KeyGroupRange getKeyGroupRange() {
			return keyGroupRange;
		}

		/**
		 * Returns the name under which the KvState has been registered.
		 *
		 * @return Name under which the KvState has been registered
		 */
		public String getRegistrationName() {
			return registrationName;
		}

		@Override
		public String toString() {
			return "NotifyKvStateUnregistered{" +
					"jobId=" + jobId +
					", jobVertexId=" + jobVertexId +
					", keyGroupRange=" + keyGroupRange +
					", registrationName='" + registrationName + '\'' +
					'}';
		}
	}

}
