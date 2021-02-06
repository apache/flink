package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

import static org.apache.flink.configuration.ConfigOptions.key;

public class RemoteHeapConfigurationOptions {
	public static final ConfigOption<MemorySize> WRITE_BATCH_SIZE =
		key("state.backend.remoteheap.write-batch-size")
			.memoryType()
			.defaultValue(MemorySize.parse("2mb"))
			.withDescription("The max size of the consumed memory for Remote Heap batch write, " +
				"will flush just based on item count if this config set to 0.");
}
