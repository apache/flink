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

package eu.stratosphere.pact.runtime.iterative.playing.simple;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

abstract class AppendMapper extends MapStub {

  @Override
  public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
    String value = record.getField(0, PactString.class).getValue();
    value += appendix();
    record.setField(0, new PactString(value));
    out.collect(record);
  }

  abstract String appendix();

  public static class AppendHeadMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Head";
    }
  }

  public static class AppendIntermediateMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Intermediate";
    }
  }

  public static class AppendTailMapper extends AppendMapper {
    @Override
    String appendix() {
      return "-Tail";
    }
  }
}
