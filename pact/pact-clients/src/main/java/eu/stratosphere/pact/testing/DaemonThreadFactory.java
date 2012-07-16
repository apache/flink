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
package eu.stratosphere.pact.testing;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @author Arvid Heise
 */
public class DaemonThreadFactory implements ThreadFactory {
	final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

	@Override
	public Thread newThread(final Runnable r) {
		Thread thread = this.defaultFactory.newThread(r);
		thread.setDaemon(true);
		return thread;
	}
}
