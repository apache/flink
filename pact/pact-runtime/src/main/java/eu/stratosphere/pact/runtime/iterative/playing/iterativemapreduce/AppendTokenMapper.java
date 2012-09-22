/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.playing.iterativemapreduce;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class AppendTokenMapper extends MapStub {

  @Override
  public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {

    PactInteger key = record.getField(0, PactInteger.class);
    key.setValue(key.getValue() * 3);
    record.setField(0, key);

    PactInteger value = record.getField(1, PactInteger.class);
    value.setValue(value.getValue() + 1);
    record.setField(1, value);

    collector.collect(record);
  }
}
