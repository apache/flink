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

package org.apache.flink.mesos.runtime.clusterframework;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;

public class MesosTaskManagerParametersTest {


    @Test
    public void givenTwoConstraintsInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters = MesosTaskManagerParameters.create(withConfiguration("cluster:foo,az:eu-west-1"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(2));
        ConstraintEvaluator firstConstraintEvaluator = new HostAttrValueConstraint("cluster", new Func1<String, String>() {
            @Override
            public String call(String s) {
                return "foo";
            }
        });
        ConstraintEvaluator secondConstraintEvaluator = new HostAttrValueConstraint("az", new Func1<String, String>() {
            @Override
            public String call(String s) {
                return "foo";
            }
        });
        assertThat(mesosTaskManagerParameters.constraints().get(0).getName(), is(firstConstraintEvaluator.getName()));
        assertThat(mesosTaskManagerParameters.constraints().get(1).getName(), is(secondConstraintEvaluator.getName()));

    }

    @Test
    public void givenOneConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters = MesosTaskManagerParameters.create(withConfiguration("cluster:foo"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(1));
        ConstraintEvaluator firstConstraintEvaluator = new HostAttrValueConstraint("cluster", new Func1<String, String>() {
            @Override
            public String call(String s) {
                return "foo";
            }
        });
        assertThat(mesosTaskManagerParameters.constraints().get(0).getName(), is(firstConstraintEvaluator.getName()));
    }

    @Test
    public void givenEmptyConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters = MesosTaskManagerParameters.create(withConfiguration(""));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(0));
    }

    @Test
    public void givenInvalidConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters = MesosTaskManagerParameters.create(withConfiguration(",:,"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(0));
    }


    private static Configuration withConfiguration(final String configuration) {
        return new Configuration() {
            private static final long serialVersionUID = -3249384117909445760L;

            {
                setString(MesosTaskManagerParameters.MESOS_CONSTRAINTS_HARD_HOSTATTR, configuration);
            }
        };
    }

}
