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

package org.apache.flink.api.common.attribute;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/** {@link Attribute} contains the information about the process logic of a process function. */
@Internal
public class Attribute implements Serializable {

    private boolean isNoOutputUntilEndOfInput;

    private Attribute(boolean isNoOutputUntilEndOfInput) {
        this.isNoOutputUntilEndOfInput = isNoOutputUntilEndOfInput;
    }

    public boolean isNoOutputUntilEndOfInput() {
        return isNoOutputUntilEndOfInput;
    }

    public void setNoOutputUntilEndOfInput(boolean noOutputUntilEndOfInput) {
        isNoOutputUntilEndOfInput = noOutputUntilEndOfInput;
    }

    @Internal
    public static class Builder {

        private boolean isNoOutputUntilEndOfInput = false;

        public Builder setNoOutputUntilEndOfInput(boolean isNoOutputUntilEndOfInput) {
            this.isNoOutputUntilEndOfInput = isNoOutputUntilEndOfInput;
            return this;
        }

        public Attribute build() {
            return new Attribute(isNoOutputUntilEndOfInput);
        }
    }
}
