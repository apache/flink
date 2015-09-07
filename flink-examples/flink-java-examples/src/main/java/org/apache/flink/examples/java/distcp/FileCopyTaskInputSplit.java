package org.apache.flink.examples.java.distcp;

import org.apache.flink.core.io.InputSplit;

/**
 * Implementation of {@code InputSplit} for copying files
 */
public class FileCopyTaskInputSplit implements InputSplit {
    private int splitNo = 0;
    private FileCopyTask task;

    public FileCopyTaskInputSplit(FileCopyTask task) {
        this.task = task;
    }

    public FileCopyTask getTask() {
        return task;
    }

    @Override
    public int getSplitNumber() {
        return splitNo++;
    }
}
