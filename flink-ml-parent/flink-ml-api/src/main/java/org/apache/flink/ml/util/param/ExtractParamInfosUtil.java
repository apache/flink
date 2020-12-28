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

package org.apache.flink.ml.util.param;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.WithParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/** Utility to extract all ParamInfos defined in a WithParams, mainly used in persistence. */
public final class ExtractParamInfosUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractParamInfosUtil.class);

    /**
     * Extracts all ParamInfos defined in the given WithParams, including those in its superclasses
     * and interfaces.
     *
     * @param s the WithParams to extract ParamInfos from
     * @return the list of all ParamInfos defined in s
     */
    public static List<ParamInfo> extractParamInfos(WithParams s) {
        return extractParamInfos(s, s.getClass());
    }

    private static List<ParamInfo> extractParamInfos(WithParams s, Class clz) {
        List<ParamInfo> result = new ArrayList<>();
        if (clz == null) {
            return result;
        }

        Field[] fields = clz.getDeclaredFields();
        for (Field f : fields) {
            f.setAccessible(true);
            if (ParamInfo.class.isAssignableFrom(f.getType())) {
                try {
                    result.add((ParamInfo) f.get(s));
                } catch (IllegalAccessException e) {
                    LOG.warn("Failed to extract param info {}, ignore it", f.getName(), e);
                }
            }
        }

        result.addAll(extractParamInfos(s, clz.getSuperclass()));
        for (Class c : clz.getInterfaces()) {
            result.addAll(extractParamInfos(s, c));
        }

        return result;
    }
}
