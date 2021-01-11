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

package org.apache.flink.ml.api.misc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.ExpectedException;

/** Test for the behavior and validator of {@link Params}. */
public class ParamsTest {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testDefaultBehavior() {
        Params params = new Params();

        ParamInfo<String> optionalWithoutDefault =
                ParamInfoFactory.createParamInfo("a", String.class).build();

        // It should call params.contain to check when get the param in this case.
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cannot find default value for optional parameter a");
        params.get(optionalWithoutDefault);

        ParamInfo<String> optionalWithDefault =
                ParamInfoFactory.createParamInfo("a", String.class)
                        .setHasDefaultValue("def")
                        .build();
        assert params.get(optionalWithDefault).equals("def");

        ParamInfo<String> requiredWithDefault =
                ParamInfoFactory.createParamInfo("a", String.class)
                        .setRequired()
                        .setHasDefaultValue("def")
                        .build();
        assert params.get(requiredWithDefault).equals("def");

        ParamInfo<String> requiredWithoutDefault =
                ParamInfoFactory.createParamInfo("a", String.class).setRequired().build();
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("a not exist which is not optional and don't have a default value");
        params.get(requiredWithoutDefault);
    }

    @Test
    public void testValidator() {
        Params params = new Params();

        ParamInfo<Integer> intParam =
                ParamInfoFactory.createParamInfo("a", Integer.class)
                        .setValidator(i -> i > 0)
                        .build();
        params.set(intParam, 1);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Setting a as a invalid value:0");
        params.set(intParam, 0);
    }

    @Test
    public void getOptionalParam() {
        ParamInfo<String> key =
                ParamInfoFactory.createParamInfo("key", String.class)
                        .setHasDefaultValue(null)
                        .setDescription("")
                        .build();

        Params params = new Params();
        Assertions.assertNull(params.get(key));

        String val = "3";
        params.set(key, val);
        Assertions.assertEquals(params.get(key), val);

        params.set(key, null);
        Assertions.assertNull(params.get(key));
    }

    @Test
    public void getOptionalWithoutDefaultParam() {
        ParamInfo<String> key =
                ParamInfoFactory.createParamInfo("key", String.class)
                        .setOptional()
                        .setDescription("")
                        .build();
        Params params = new Params();

        try {
            String val = params.get(key);
            Assertions.fail("Should throw exception.");
        } catch (IllegalArgumentException ex) {
            Assertions.assertTrue(
                    ex.getMessage().startsWith("Cannot find default value for optional parameter"));
        }

        Assertions.assertFalse(params.contains(key));

        String val = "3";
        params.set(key, val);
        Assertions.assertEquals(params.get(key), val);

        Assertions.assertTrue(params.contains(key));

        params.set(key, null);
        Assertions.assertNull(params.get(key));
    }

    @Test
    public void getRequiredParam() {
        ParamInfo<String> labelWithRequired =
                ParamInfoFactory.createParamInfo("label", String.class)
                        .setDescription("")
                        .setRequired()
                        .build();
        Params params = new Params();
        try {
            params.get(labelWithRequired);
            Assertions.fail("failure");
        } catch (IllegalArgumentException ex) {
            Assertions.assertTrue(ex.getMessage().startsWith("Missing non-optional parameter"));
        }

        params.set(labelWithRequired, null);
        Assertions.assertNull(params.get(labelWithRequired));

        String val = "3";
        params.set(labelWithRequired, val);
        Assertions.assertEquals(params.get(labelWithRequired), val);
    }

    @Test
    public void testGetAliasParam() {
        ParamInfo<String> predResultColName =
                ParamInfoFactory.createParamInfo("predResultColName", String.class)
                        .setDescription("Column name of predicted result.")
                        .setRequired()
                        .setAlias(new String[] {"predColName", "outputColName"})
                        .build();

        Params params = Params.fromJson("{\"predResultColName\":\"\\\"f0\\\"\"}");

        Assertions.assertEquals("f0", params.get(predResultColName));

        params =
                Params.fromJson(
                        "{\"predResultColName\":\"\\\"f0\\\"\", \"predColName\":\"\\\"f0\\\"\"}");

        try {
            params.get(predResultColName);
            Assertions.fail("failure");
        } catch (IllegalArgumentException ex) {
            Assertions.assertTrue(
                    ex.getMessage()
                            .startsWith(
                                    "Duplicate parameters of predResultColName and predColName"));
        }
    }
}
