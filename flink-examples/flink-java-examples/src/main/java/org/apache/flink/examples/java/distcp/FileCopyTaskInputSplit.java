package org.apache.flink.examples.java.distcp;

import org.apache.flink.core.io.InputSplit;

/**
 * Created by zholudev on 27/08/15.
 * Input split for copying files
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
