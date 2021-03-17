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

package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import static java.util.Objects.requireNonNull;

/**
 * Message sent by the callback handler to the scheduler actor when the scheduler re-registers with
 * a newly elected Mesos master.
 */
public class ReRegistered extends Connected {

    private static final long serialVersionUID = 1L;

    private final Protos.MasterInfo masterInfo;

    public ReRegistered(Protos.MasterInfo masterInfo) {
        requireNonNull(masterInfo);

        this.masterInfo = masterInfo;
    }

    public Protos.MasterInfo masterInfo() {
        return masterInfo;
    }

    @Override
    public String toString() {
        return "ReRegistered{" + "masterInfo=" + masterInfo + '}';
    }
}
