package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;

public interface DynamicHybridSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {
    /**
     * Gets a list of finished splits for this reader.
     *
     * <p>This method is called by {@link HybridSourceReader} during checkpoint for persistent, and
     * source switch to dynamically initiate the start positions of the next source.
     *
     * <p>The contract is that implementation should ensure the new finished splits since the last
     * call of this method persist until the next checkpoint.
     */
    List<SplitT> getFinishedSplits();
}
