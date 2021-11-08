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

import org.apache.flink.core.io.InputSplit;

/** Implementation of {@code InputSplit} for copying files. */
public class FileCopyTaskInputSplit implements InputSplit {

    private static final long serialVersionUID = -7621656017747660450L;

    private final FileCopyTask task;
    private final int splitNumber;

    public FileCopyTaskInputSplit(FileCopyTask task, int splitNumber) {
        this.task = task;
        this.splitNumber = splitNumber;
    }

    public FileCopyTask getTask() {
        return task;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
