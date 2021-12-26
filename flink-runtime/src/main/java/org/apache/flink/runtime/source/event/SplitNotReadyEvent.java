package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/**
 * event for split not ready.
 */
public class SplitNotReadyEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "[SplitNotReadyEvent]";
    }

}
