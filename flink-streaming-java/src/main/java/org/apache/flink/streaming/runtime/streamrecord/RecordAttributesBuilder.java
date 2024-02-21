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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Experimental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** The builder class for {@link RecordAttributes}. */
@Experimental
public class RecordAttributesBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(RecordAttributesBuilder.class);
    private final List<RecordAttributes> lastRecordAttributesOfInputs;
    @Nullable private Boolean isBacklog = null;

    /**
     * This constructor takes a list of the last RecordAttributes received from each of the
     * operator's inputs. Each input is corresponding to an input edge of the job graph. When this
     * list is not empty, it will be used to determine the default values for those attributes that
     * have not been explicitly set by caller.
     */
    public RecordAttributesBuilder(List<RecordAttributes> lastRecordAttributesOfInputs) {
        this.lastRecordAttributesOfInputs = lastRecordAttributesOfInputs;
    }

    public RecordAttributesBuilder setBacklog(boolean isBacklog) {
        this.isBacklog = isBacklog;
        return this;
    }

    /**
     * If any operator attribute is null, we will log it at DEBUG level and determine a non-null
     * default value as described below.
     *
     * <p>Default value for backlog: if any element in lastRecordAttributesOfInputs has
     * backlog=true, use true. Otherwise, use false.
     */
    public RecordAttributes build() {
        if (isBacklog == null) {
            final boolean defaultBacklog = getDefaultBacklog();
            LOG.debug(
                    "backlog is not set, set to {} from the last record attributes {}.",
                    defaultBacklog,
                    lastRecordAttributesOfInputs);
            isBacklog = defaultBacklog;
        }
        return new RecordAttributes(isBacklog);
    }

    private boolean getDefaultBacklog() {
        for (RecordAttributes lastAttributes : lastRecordAttributesOfInputs) {
            if (lastAttributes.isBacklog()) {
                return true;
            }
        }
        return false;
    }
}
