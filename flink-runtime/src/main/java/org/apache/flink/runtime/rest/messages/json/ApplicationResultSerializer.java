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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.runtime.highavailability.ApplicationResult;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON serializer for {@link ApplicationResult}.
 *
 * @see ApplicationResultDeserializer
 */
public class ApplicationResultSerializer extends StdSerializer<ApplicationResult> {

    private static final long serialVersionUID = 1L;

    static final String FIELD_NAME_APPLICATION_ID = "application-id";

    static final String FIELD_NAME_APPLICATION_STATE = "application-state";

    static final String FIELD_NAME_APPLICATION_NAME = "application-name";

    static final String FIELD_NAME_START_TIME = "start-time";

    static final String FIELD_NAME_END_TIME = "end-time";

    private final ApplicationIDSerializer applicationIdSerializer = new ApplicationIDSerializer();

    public ApplicationResultSerializer() {
        super(ApplicationResult.class);
    }

    @Override
    public void serialize(
            final ApplicationResult result,
            final JsonGenerator gen,
            final SerializerProvider provider)
            throws IOException {

        gen.writeStartObject();

        gen.writeFieldName(FIELD_NAME_APPLICATION_ID);
        applicationIdSerializer.serialize(result.getApplicationId(), gen, provider);

        gen.writeFieldName(FIELD_NAME_APPLICATION_STATE);
        gen.writeString(result.getApplicationState().name());

        gen.writeFieldName(FIELD_NAME_APPLICATION_NAME);
        gen.writeString(result.getApplicationName());

        gen.writeNumberField(FIELD_NAME_START_TIME, result.getStartTime());
        gen.writeNumberField(FIELD_NAME_END_TIME, result.getEndTime());

        gen.writeEndObject();
    }
}
