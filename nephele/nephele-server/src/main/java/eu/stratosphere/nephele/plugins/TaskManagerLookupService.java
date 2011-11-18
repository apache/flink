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
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.taskmanager.TaskManager;

public final class TaskManagerLookupService implements PluginLookupService {

	private final TaskManager taskManager;

	public TaskManagerLookupService(final TaskManager taskManager) {
		this.taskManager = taskManager;
	}

	private static final class JobManagerStub implements PluginCommunication {

		private final TaskManager taskManager;

		private final PluginID pluginID;

		public JobManagerStub(final TaskManager taskManager, final PluginID pluginID) {
			this.taskManager = taskManager;
			this.pluginID = pluginID;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void sendData(final IOReadableWritable data) throws IOException {

			this.taskManager.sendData(this.pluginID, data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void sendData(final List<IOReadableWritable> listOfData) throws IOException {

			this.taskManager.sendData(this.pluginID, listOfData);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

			return this.taskManager.requestData(this.pluginID, data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public IOReadableWritable requestData(final List<IOReadableWritable> listOfData) throws IOException {

			return this.taskManager.requestData(this.pluginID, listOfData);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public PluginCommunication getJobManagerComponent(final PluginID pluginID) {

		return new JobManagerStub(this.taskManager, pluginID);
	}
}
