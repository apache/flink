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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;

/**
 * Abstract factory for a web runtime monitor that is dynamically loaded.
 */
public abstract class WebRuntimeMonitorFactory {

	/**
	 * Starts the web runtime monitor.
	 * 
	 * @param config The system configuration.
	 * @param jobManager Reference to the JobManager actor.
	 * @param archive Reference to the Archive actor.
	 * @throws Exception Thrown if the initialization fails.
	 */
	public abstract void startWebRuntimeMonitor(Configuration config, ActorRef jobManager, ActorRef archive)
			throws Exception;

	// --------------------------------------------------------------------------------------------
	//  Dynamic loading
	// --------------------------------------------------------------------------------------------

	/**
	 * Tries to load and instantiate the class implementing the WebRuntimeMonitorFactory. This method
	 * does not throw an exception, but returns {@code null} when the WebRuntimeMonitorFactory
	 * cannot be created. Problems are logged to the given logger.
	 *
	 * @param className The name of the class implementing the WebRuntimeMonitorFactory.
	 * @param logger The logger to log errors to. May be null.
	 *
	 * @return The WebRuntimeMonitorFactory, ir {@code null}, if it could not be loaded.
	 */
	public static WebRuntimeMonitorFactory tryLoadFactory(String className, Logger logger) {
		return tryLoadFactory(className, ClassLoader.getSystemClassLoader(), logger);
	}

	/**
	 * Tries to load and instantiate the class implementing the WebRuntimeMonitorFactory. This method
	 * does not throw an exception, but returns {@code null} when the WebRuntimeMonitorFactory
	 * cannot be created. Problems are logged to the given logger.
	 *
	 * @param className The name of the class implementing the WebRuntimeMonitorFactory.
	 * @param loader The class loader to load the WebRuntimeMonitorFactory from.
	 * @param logger The logger to log errors to. May be null.
	 *
	 * @return The WebRuntimeMonitorFactory, ir {@code null}, if it could not be loaded.
	 */
	public static WebRuntimeMonitorFactory tryLoadFactory(String className, ClassLoader loader, Logger logger) {
		try {
			Class<? extends WebRuntimeMonitorFactory> factoryClass = Class.forName(className, true, loader)
																		.asSubclass(WebRuntimeMonitorFactory.class);

			return InstantiationUtil.instantiate(factoryClass);
		}
		catch (ClassNotFoundException e) {
			if (logger != null) {
				logger.error("Cannot load the web runtime monitor classes. " +
						"The runtime monitor is probably not in the classpath");
				logger.debug("Runtime monitor class loading error", e);
			}
			return null;
		}
		catch (ClassCastException e) {
			if (logger != null) {
				logger.error("The given class is not a proper web runtime monitor factory. Class must be subclass of " +
						WebRuntimeMonitorFactory.class.getName(), e);
			}
			return null;
		}
		catch (Exception e) {
			if (logger != null) {
				logger.error("Cannot create web runtime monitor factory: " + e.getMessage(), e);
			}
			return null;
		}
	}
}
