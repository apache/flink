/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.client.resource;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.MutableURLClassLoader;

import java.net.URL;

/**
 * This is only used by SqlClient, which expose {@code removeURL} method to support {@code REMOVE
 * JAR} clause.
 */
@Internal
public class ClientResourceManager extends ResourceManager {

    public ClientResourceManager(Configuration config, MutableURLClassLoader userClassLoader) {
        super(config, userClassLoader);
    }

    /**
     * The method is only used to SqlClient for supporting remove jar syntax. SqlClient must
     * guarantee also remove the jar from userClassLoader because it is {@code
     * ClientMutableURLClassLoader}.
     */
    public URL unregisterJarResource(String jarPath) {
        return resourceInfos.remove(new ResourceUri(ResourceType.JAR, jarPath));
    }
}
