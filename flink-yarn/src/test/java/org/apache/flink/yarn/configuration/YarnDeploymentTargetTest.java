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

package org.apache.flink.yarn.configuration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link YarnDeploymentTarget}. */
public class YarnDeploymentTargetTest {

    @Test
    public void testCorrectInstantiationFromConfiguration() {
        for (YarnDeploymentTarget t : YarnDeploymentTarget.values()) {
            testCorrectInstantiationFromConfigurationHelper(t);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInstantiationFromConfiguration() {
        final Configuration configuration = getConfigurationWithTarget("invalid-target");
        YarnDeploymentTarget.fromConfig(configuration);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInstantiationFromConfiguration() {
        YarnDeploymentTarget.fromConfig(new Configuration());
    }

    @Test
    public void testThatAValidOptionIsValid() {
        assertTrue(
                YarnDeploymentTarget.isValidYarnTarget(YarnDeploymentTarget.APPLICATION.getName()));
    }

    @Test
    public void testThatAnInvalidOptionIsInvalid() {
        assertFalse(YarnDeploymentTarget.isValidYarnTarget("invalid-target"));
    }

    private void testCorrectInstantiationFromConfigurationHelper(
            final YarnDeploymentTarget expectedDeploymentTarget) {
        final Configuration configuration =
                getConfigurationWithTarget(expectedDeploymentTarget.getName().toUpperCase());
        final YarnDeploymentTarget actualDeploymentTarget =
                YarnDeploymentTarget.fromConfig(configuration);

        assertSame(actualDeploymentTarget, expectedDeploymentTarget);
    }

    private Configuration getConfigurationWithTarget(final String target) {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, target);
        return configuration;
    }
}
