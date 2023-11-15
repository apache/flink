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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.NetUtils;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Represents a port range and provides access to an iterator of the given range. */
public class PortRange {

    private final String portRange;
    private final Iterator<Integer> portsIterator;

    public PortRange(int port) {
        this(String.valueOf(port));
    }

    /**
     * Creates a new port range instance.
     *
     * @param portRange given port range string
     * @throws IllegalConfigurationException if given port range string is invalid
     */
    public PortRange(String portRange) {
        this.portRange = checkNotNull(portRange);
        try {
            portsIterator = NetUtils.getPortRangeFromString(portRange);
        } catch (NumberFormatException e) {
            throw new IllegalConfigurationException("Invalid port range: \"" + portRange + "\"");
        }
    }

    public Iterator<Integer> getPortsIterator() {
        return portsIterator;
    }

    @Override
    public String toString() {
        return portRange;
    }
}
