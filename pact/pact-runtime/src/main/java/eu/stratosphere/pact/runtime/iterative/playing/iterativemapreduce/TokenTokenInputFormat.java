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

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import java.util.regex.Pattern;

public class TokenTokenInputFormat extends TextInputFormat {

  private static final Pattern SEPARATOR = Pattern.compile(",");

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int numBytes) {
    String str = new String(bytes, 0, numBytes);
    String[] parts = SEPARATOR.split(str);

    target.clear();
    target.addField(new PactString(parts[0]));
    target.addField(new PactString(parts[1]));

    return true;
  }
}
