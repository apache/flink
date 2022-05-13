/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.distcp;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * An implementation of an input format that dynamically assigns {@code FileCopyTask} to the mappers
 * that have finished previously assigned tasks.
 */
public class FileCopyTaskInputFormat implements InputFormat<FileCopyTask, FileCopyTaskInputSplit> {

    private static final long serialVersionUID = -644394866425221151L;

    private static final Logger LOGGER = LoggerFactory.getLogger(FileCopyTaskInputFormat.class);

    private final List<FileCopyTask> tasks;

    public FileCopyTaskInputFormat(List<FileCopyTask> tasks) {
        this.tasks = tasks;
    }

    private class FileCopyTaskAssigner implements InputSplitAssigner {
        private Queue<FileCopyTaskInputSplit> splits;

        public FileCopyTaskAssigner(FileCopyTaskInputSplit[] inputSplits) {
            splits = new LinkedList<>(Arrays.asList(inputSplits));
        }

        @Override
        public InputSplit getNextInputSplit(String host, int taskId) {
            LOGGER.info("Getting copy task for task: " + taskId);
            return splits.poll();
        }

        @Override
        public void returnInputSplit(List<InputSplit> splits, int taskId) {
            synchronized (this.splits) {
                for (InputSplit split : splits) {
                    Preconditions.checkState(this.splits.add((FileCopyTaskInputSplit) split));
                }
            }
        }
    }

    @Override
    public void configure(Configuration parameters) {
        // no op
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public FileCopyTaskInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        FileCopyTaskInputSplit[] splits = new FileCopyTaskInputSplit[tasks.size()];
        int i = 0;
        for (FileCopyTask t : tasks) {
            splits[i] = new FileCopyTaskInputSplit(t, i);
            i++;
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(FileCopyTaskInputSplit[] inputSplits) {
        return new FileCopyTaskAssigner(inputSplits);
    }

    private FileCopyTaskInputSplit curInputSplit = null;

    @Override
    public void open(FileCopyTaskInputSplit split) throws IOException {
        curInputSplit = split;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return curInputSplit == null;
    }

    @Override
    public FileCopyTask nextRecord(FileCopyTask reuse) throws IOException {
        FileCopyTask toReturn = curInputSplit.getTask();
        curInputSplit = null;
        return toReturn;
    }

    @Override
    public void close() throws IOException {
        // no op
    }
}
