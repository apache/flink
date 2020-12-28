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

package org.apache.flink.yarn.util;

import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContextInitializeException;
import org.apache.flink.runtime.security.contexts.HadoopSecurityContext;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.security.contexts.SecurityContextFactory;

import org.apache.hadoop.security.UserGroupInformation;

/** Test hadoop context module factory associated with {@link TestHadoopModuleFactory}. */
public class TestHadoopSecurityContextFactory implements SecurityContextFactory {

    @Override
    public boolean isCompatibleWith(SecurityConfiguration securityConfig) {
        return securityConfig
                .getSecurityModuleFactories()
                .contains(TestHadoopModuleFactory.class.getCanonicalName());
    }

    @Override
    public SecurityContext createContext(SecurityConfiguration securityConfig)
            throws SecurityContextInitializeException {
        try {
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            return new HadoopSecurityContext(loginUser);
        } catch (Exception e) {
            throw new SecurityContextInitializeException(
                    "Cannot instantiate test security context", e);
        }
    }
}
