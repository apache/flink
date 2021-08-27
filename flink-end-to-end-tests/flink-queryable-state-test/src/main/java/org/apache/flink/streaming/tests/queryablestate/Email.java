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

package org.apache.flink.streaming.tests.queryablestate;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Toy email resentation. */
public class Email {

    private EmailId emailId;
    private Instant timestamp;
    private String foo;
    private LabelSurrogate label;

    public Email(EmailId emailId, Instant timestamp, String foo, LabelSurrogate label) {
        this.emailId = emailId;
        this.timestamp = timestamp;
        this.foo = foo;
        this.label = label;
    }

    public EmailId getEmailId() {
        return emailId;
    }

    public void setEmailId(EmailId emailId) {
        this.emailId = emailId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getFoo() {
        return foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    public LabelSurrogate getLabel() {
        return label;
    }

    public void setLabel(LabelSurrogate label) {
        this.label = label;
    }

    public String getDate() {
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
        return formatter.format(timestamp);
    }
}
