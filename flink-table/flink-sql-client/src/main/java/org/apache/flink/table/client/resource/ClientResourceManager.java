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

package org.apache.flink.table.client.resource;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.MutableURLClassLoader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;

/**
 * The {@link ClientResourceManager} is able to remove the registered JAR resources with the
 * specified jar path.
 *
 * <p>After removing the JAR resource, the {@link ResourceManager} is able to register the JAR
 * resource with the same JAR path. Please notice that the removal doesn't promise the loaded {@link
 * Class} from the removed jar is inaccessible.
 */
@Internal
public class ClientResourceManager extends ResourceManager {

    public ClientResourceManager(Configuration config, MutableURLClassLoader userClassLoader) {
        super(config, userClassLoader);
    }

    @Nullable
    public URL unregisterJarResource(String jarPath) {
        Path path = new Path(jarPath);
        try {
            checkPath(path, ResourceType.JAR);
            return resourceInfos.remove(
                    new ResourceUri(ResourceType.JAR, getURLFromPath(path).getPath()));
        } catch (IOException e) {
            throw new SqlExecutionException(
                    String.format("Failed to unregister the jar resource [%s]", jarPath), e);
        }
    }
}
