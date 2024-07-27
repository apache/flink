/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.SplittableIterator;

import java.util.Iterator;

/**
 * A {@link SourceFunction} that reads elements from an {@link SplittableIterator} and emits them.
 *
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@Deprecated
@PublicEvolving
public class FromSplittableIteratorFunction<T> extends RichParallelSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private SplittableIterator<T> fullIterator;

    private transient Iterator<T> iterator;

    private volatile boolean isRunning = true;

    public FromSplittableIteratorFunction(SplittableIterator<T> iterator) {
        this.fullIterator = iterator;
    }

    /**
     * Initialization method for the {@link FromSplittableIteratorFunction}.
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception if an error happens.
     * @deprecated This method is deprecated since Flink 1.19. The users are recommended to
     *     implement {@code open(OpenContext openContext)} and override {@code open(Configuration
     *     parameters)} with an empty body instead. 1. If you implement {@code open(OpenContext
     *     openContext)}, the {@code open(OpenContext openContext)} will be invoked and the {@code
     *     open(Configuration parameters)} won't be invoked. 2. If you don't implement {@code
     *     open(OpenContext openContext)}, the {@code open(Configuration parameters)} will be
     *     invoked in the default implementation of the {@code open(OpenContext openContext)}.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=263425231">
     *     FLIP-344: Remove parameter in RichFunction#open </a>
     */
    @Deprecated
    @Override
    public void open(Configuration parameters) throws Exception {
        int numberOfSubTasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        int indexofThisSubTask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        iterator = fullIterator.split(numberOfSubTasks)[indexofThisSubTask];
        isRunning = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (isRunning && iterator.hasNext()) {
            ctx.collect(iterator.next());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
