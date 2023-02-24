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

package org.apache.flink.runtime.security.modules;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HadoopModule}. */
class HadoopModuleTest {
    @Test
    public void startTGTRenewalShouldScheduleRenewalWithKeytab() throws IOException {
        final ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);

        Configuration flinkConf = new Configuration();
        SecurityConfiguration securityConf = new SecurityConfiguration(flinkConf);
        org.apache.hadoop.conf.Configuration hadoopConf =
                new org.apache.hadoop.conf.Configuration();
        HadoopModule hadoopModule = new HadoopModule(securityConf, hadoopConf);

        hadoopModule.startTGTRenewal(executorService, userGroupInformation);
        executorService.triggerPeriodicScheduledTasks();
        hadoopModule.stopTGTRenewal();

        verify(userGroupInformation, times(1)).checkTGTAndReloginFromKeytab();
    }

    @Test
    public void hadoopProxyUserSetWithDelegationTokensEnabledShouldThrow() {
        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);
            ugi.when(UserGroupInformation::getLoginUser).thenReturn(userGroupInformation);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.PROXY);
            Configuration flinkConf = new Configuration();
            flinkConf.set(SecurityOptions.DELEGATION_TOKENS_ENABLED, true);
            SecurityConfiguration securityConf = new SecurityConfiguration(flinkConf);
            org.apache.hadoop.conf.Configuration hadoopConf =
                    new org.apache.hadoop.conf.Configuration();
            HadoopModule hadoopModule = new HadoopModule(securityConf, hadoopConf);
            Exception exception =
                    assertThrows(
                            SecurityModule.SecurityInstallException.class, hadoopModule::install);
            assertTrue(exception.getCause() instanceof UnsupportedOperationException);
        }
    }
}
