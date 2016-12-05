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

package org.apache.flink.cep.examples.functions;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.examples.events.MonitoringEvent;
import org.apache.flink.cep.examples.events.TemperatureEvent;
import org.apache.flink.cep.examples.events.TemperatureWarning;

import java.util.Map;

public class TemperatureWarningPatternSelectFunction
	implements PatternSelectFunction<MonitoringEvent, TemperatureWarning> {

    @Override
    public TemperatureWarning select(Map<String, MonitoringEvent> pattern) throws Exception {
        TemperatureEvent first = (TemperatureEvent) pattern.get("first");
        TemperatureEvent second = (TemperatureEvent) pattern.get("second");

        return new TemperatureWarning(
            first.getRackID(),
            (first.getTemperature() + second.getTemperature()) / 2
        );
    }
}
