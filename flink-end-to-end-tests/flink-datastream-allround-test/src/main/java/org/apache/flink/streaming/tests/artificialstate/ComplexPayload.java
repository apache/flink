/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.artificialstate;

import org.apache.flink.streaming.tests.DataStreamAllroundTestProgram;
import org.apache.flink.streaming.tests.Event;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * A state type used in the {@link DataStreamAllroundTestProgram}. Wraps an {@link Event} as state.
 */
public class ComplexPayload implements Serializable {
    private static final long serialVersionUID = 233624606545704853L;

    private final long eventTime;
    private final List<String> stringList;
    private final String strPayload;
    private final InnerPayLoad innerPayLoad;

    public ComplexPayload(Event event, String strPayload) {
        this.eventTime = event.getEventTime();
        this.innerPayLoad = new InnerPayLoad(event.getSequenceNumber());
        this.strPayload = strPayload;
        this.stringList = Arrays.asList(String.valueOf(event.getKey()), event.getPayload());
    }

    public ComplexPayload(Event event) {
        this(event, event.getPayload());
    }

    /** Nested class in state type. Wraps an {@link Event}'s sequence number. */
    public static class InnerPayLoad implements Serializable {

        private static final long serialVersionUID = 3986298180012117883L;

        private final long sequenceNumber;

        public InnerPayLoad(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }
    }

    public long getEventTime() {
        return eventTime;
    }

    public List<String> getStringList() {
        return stringList;
    }

    public String getStrPayload() {
        return strPayload;
    }

    public InnerPayLoad getInnerPayLoad() {
        return innerPayLoad;
    }
}
