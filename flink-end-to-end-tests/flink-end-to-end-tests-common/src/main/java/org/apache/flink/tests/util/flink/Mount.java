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

package org.apache.flink.tests.util.flink;

/** Represents a mount of a file or a directory from the local file system into a test container. */
public class Mount {

    private final String from;
    private final String to;

    private Mount(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public static Mount of(String from, String to) {
        return new Mount(from, to);
    }

    public String from() {
        return from;
    }

    public String to() {
        return to;
    }
}
