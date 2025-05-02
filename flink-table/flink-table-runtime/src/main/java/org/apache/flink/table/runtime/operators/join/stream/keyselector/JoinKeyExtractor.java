package org.apache.flink.table.runtime.operators.join.stream.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.StreamingMultiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.Serializable;

/**
 * Defines the strategy for extracting keys used for state management within the {@link
 * StreamingMultiJoinOperator}.
 *
 * <p>Implementations define how to derive keys for storing state records and for looking up
 * matching records in state during the join process.
 */
public interface JoinKeyExtractor extends Serializable {

    /**
     * Extracts the key used for storing the input record in its corresponding state view.
     *
     * <p>This key determines how records from a specific input stream are organized within their
     * state.
     *
     * @param row The input row for which to extract the storage key.
     * @param inputId The ID (0-based index) of the input stream this row belongs to.
     * @return A {@link RowData} representing the state storage key. Can be null if no key can be
     *     derived (e.g., missing configuration).
     */
    RowData getKeyForStateStorage(RowData row, int inputId);

    /**
     * Extracts the key used for looking up matching records in the state of a specific input depth,
     * based on the rows accumulated from previous inputs.
     *
     * <p>When processing the join recursively at a certain `depth`, this key is used to query the
     * state associated with that `depth` (i.e., the state for input `depth`) to find potential join
     * partners.
     *
     * @param depth The current processing depth (0-based index), representing the target input ID
     *     for state lookup.
     * @param currentRows The array of rows accumulated so far in the current recursive join path.
     *     `currentRows[0]` to `currentRows[depth-1]` contain rows from previous inputs that are
     *     relevant for constructing the lookup key.
     * @return A {@link RowData} representing the state lookup key. Can be null if no key can be
     *     derived (e.g., missing configuration, or a required row in `currentRows` is null).
     */
    RowData getKeyForStateLookup(int depth, RowData[] currentRows);

    /**
     * Determines the type information for the key associated with a specific input's state.
     *
     * <p>This is needed to correctly initialize the state backend (e.g., {@link
     * MultiJoinStateView}). The returned type should match the structure of the keys produced by
     * {@link #getKeyForStateStorage(RowData, int)} and potentially {@link
     * #getKeyForStateLookup(int, RowData[])} (depending on the implementation logic).
     *
     * @param inputId The ID (0-based index) of the input stream.
     * @return The {@link InternalTypeInfo} for the key type used by the state for this input. Can
     *     be null if no key type can be determined.
     */
    InternalTypeInfo<RowData> getKeyType(int inputId);
}
