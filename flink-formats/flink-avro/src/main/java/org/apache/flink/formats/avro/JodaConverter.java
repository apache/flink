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

package org.apache.flink.formats.avro;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * Encapsulates joda optional dependency. Instantiates this class only if joda is available on the
 * classpath.
 */
class JodaConverter {

    private static JodaConverter instance;
    private static boolean instantiated = false;

    public static JodaConverter getConverter() {
        if (instantiated) {
            return instance;
        }

        try {
            Class.forName(
                    "org.joda.time.DateTime",
                    false,
                    Thread.currentThread().getContextClassLoader());
            instance = new JodaConverter();
        } catch (ClassNotFoundException e) {
            instance = null;
        } finally {
            instantiated = true;
        }
        return instance;
    }

    public long convertDate(Object object) {
        final LocalDate value = (LocalDate) object;
        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final LocalTime value = (LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestampMillis(Object object) {
        return ((DateTime) object).getMillis();
    }

    public long convertTimestampMicros(Object object) {
        return ((DateTime) object).getMillis() * 1_000;
    }

    private JodaConverter() {}
}
