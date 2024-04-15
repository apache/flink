package org.apache.flink.externalresource.log;

import org.apache.flink.core.execution.TerminationLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTerminationLog implements TerminationLog {

    protected static final Logger LOG = LoggerFactory.getLogger(DefaultTerminationLog.class);

    /**
     * Default class used where it doesnt perform any operation
     * @param error
     */
    @Override
    public void writeTerminationLog(Throwable error, String errorCode) {
        LOG.info("Not performing any operation..!!");
        return;
    }
}
