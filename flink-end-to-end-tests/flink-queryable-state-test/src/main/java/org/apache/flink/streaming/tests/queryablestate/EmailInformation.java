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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** POJO representing some information about an email. */
public class EmailInformation implements Serializable {

    private static final long serialVersionUID = -8956979869800484909L;

    public void setEmailId(EmailId emailId) {
        this.emailId = emailId;
    }

    private EmailId emailId;

    public void setStuff(List<String> stuff) {
        this.stuff = stuff;
    }

    private List<String> stuff;

    public void setAsdf(Long asdf) {
        this.asdf = asdf;
    }

    private Long asdf = 0L;

    private transient LabelSurrogate label;

    public EmailInformation() {}

    public EmailInformation(Email email) {
        emailId = email.getEmailId();
        stuff = new ArrayList<>();
        stuff.add("1");
        stuff.add("2");
        stuff.add("3");
        label = email.getLabel();
    }

    public EmailId getEmailId() {
        return emailId;
    }

    public List<String> getStuff() {
        return stuff;
    }

    public Long getAsdf() {
        return asdf;
    }

    public LabelSurrogate getLabel() {
        return label;
    }

    public void setLabel(LabelSurrogate label) {
        this.label = label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EmailInformation that = (EmailInformation) o;
        return Objects.equals(emailId, that.emailId)
                && Objects.equals(stuff, that.stuff)
                && Objects.equals(asdf, that.asdf)
                && Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emailId, stuff, asdf, label);
    }

    @Override
    public String toString() {
        return "EmailInformation{"
                + "emailId="
                + emailId
                + ", stuff="
                + stuff
                + ", asdf="
                + asdf
                + ", label="
                + label
                + '}';
    }
}
