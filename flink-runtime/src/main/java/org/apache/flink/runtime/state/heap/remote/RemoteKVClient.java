package org.apache.flink.runtime.state.heap.remote;

import java.io.IOException;
import java.util.Collection;

public interface RemoteKVClient {
	Long dbSize();

	Collection<String> keys(String predicate);

	void openDB(String host) throws IOException;

	void closeDB();

	void pipelineSync();

	void pipelineClose();
}
