package org.apache.flink.core.execution;

import org.apache.flink.annotation.Public;

import javax.annotation.Nullable;

@Public
public interface TerminationLog {

    void writeTerminationLog(@Nullable Throwable error, @Nullable String errorCode);
}
