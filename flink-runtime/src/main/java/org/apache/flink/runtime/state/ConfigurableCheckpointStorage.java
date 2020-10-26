package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;

/**
 * An interface for checkpoint storage types that pick up additional parameters from a
 * configuration.
 */
@Internal
public interface ConfigurableCheckpointStorage extends CheckpointStorage {

    /**
     * Creates a variant of the checkpoint storage that applies additional configuration parameters.
     *
     * <p>Settings that were directly done on the original checkpoint storage object in the
     * application program typically have precedence over setting picked up from the configuration.
     *
     * <p>If no configuration is applied, or if the method directly applies configuration values to
     * the (mutable) checkpoint storage object, this method may return the original checkpoint
     * storage object. Otherwise it typically returns a modified copy.
     *
     * @param config The configuration to pick the values from.
     * @param classLoader The class loader that should be used to load the checkpoint storage.
     * @return A reconfigured checkpoint storage.
     * @throws IllegalConfigurationException Thrown if the configuration contained invalid entries.
     */
    CheckpointStorage configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException;
}
