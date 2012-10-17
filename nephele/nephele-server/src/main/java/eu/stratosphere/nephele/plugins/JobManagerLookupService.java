/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.plugins;

import java.io.IOException;

import eu.stratosphere.nephele.instance.AbstractInstance;

public final class JobManagerLookupService implements PluginLookupService {

	private final PluginCommunicationProtocol jobManager;

	JobManagerLookupService(final PluginCommunicationProtocol jobManager) {
		this.jobManager = jobManager;
	}

	private static final class JobManagerStub implements PluginCommunication {

		private final PluginCommunicationProtocol jobManager;

		private final PluginID pluginID;

		private JobManagerStub(final PluginCommunicationProtocol jobManager, final PluginID pluginID) {
			this.jobManager = jobManager;
			this.pluginID = pluginID;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void sendData(final IOReadableWritable data) throws IOException {

			synchronized (this.jobManager) {
				this.jobManager.sendData(this.pluginID, data);
			}

		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

			synchronized (this.jobManager) {
				return this.jobManager.requestData(this.pluginID, data);
			}
		}
	}

	private final static class TaskManagerStub implements PluginCommunication {

		private final AbstractInstance instance;

		private final PluginID pluginID;

		private TaskManagerStub(final AbstractInstance instance, final PluginID pluginID) {
			this.instance = instance;
			this.pluginID = pluginID;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void sendData(final IOReadableWritable data) throws IOException {

			this.instance.sendData(this.pluginID, data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

			return this.instance.requestData(this.pluginID, data);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PluginCommunication getJobManagerComponent(final PluginID pluginID) {

		return new JobManagerStub(this.jobManager, pluginID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PluginCommunication getTaskManagerComponent(final PluginID pluginID, final AbstractInstance instance) {

		return new TaskManagerStub(instance, pluginID);
	}

}
