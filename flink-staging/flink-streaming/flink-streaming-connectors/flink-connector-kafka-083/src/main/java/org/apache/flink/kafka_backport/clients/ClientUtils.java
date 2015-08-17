/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.kafka_backport.clients;

import org.apache.flink.kafka_backport.common.config.ConfigException;
import org.apache.flink.kafka_backport.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

public class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String url : urls) {
            if (url != null && url.length() > 0) {
                String host = Utils.getHost(url);
                Integer port = Utils.getPort(url);
                if (host == null || port == null)
                    throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                try {
                    InetSocketAddress address = new InetSocketAddress(host, port);
                    if (address.isUnresolved())
                        throw new ConfigException("DNS resolution failed for url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.size() < 1)
            throw new ConfigException("No bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    public static void closeQuietly(Closeable c, String name, AtomicReference<Throwable> firstException) {
        if (c != null) {
            try {
                c.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close " + name, t);
            }
        }
    }
}