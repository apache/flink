package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;

/**
 * A factory to create a specific job listener. The job listener creation gets a Configuration
 * object that can be used to read further config values.
 *
 * <p>The job listener factory is typically specified in the configuration to produce a
 * configured job listener.
 *
 * @param <T> The type of the job listener created.
 */
@PublicEvolving
public interface JobListenerFactory<T extends JobListener> {

	/**
	 * Creates the job listener, optionally using the given configuration.
	 *
	 * @param config      The Flink configuration (loaded by the TaskManager).
	 * @param classLoader The class loader that should be used to load the job listener.
	 * @return The created job listener.
	 * @throws IllegalConfigurationException If the configuration misses critical values, or specifies invalid values
	 */
	T createFromConfig(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException;
}
