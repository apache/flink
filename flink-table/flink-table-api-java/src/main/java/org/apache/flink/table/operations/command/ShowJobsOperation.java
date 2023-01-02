package org.apache.flink.table.operations.command;

import org.apache.flink.table.operations.Operation;

/** Operation to describe a SHOW JOBS statement. */
public class ShowJobsOperation implements Operation {

    @Override
    public String asSummaryString() {
        return "SHOW JOBS";
    }
}
