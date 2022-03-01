/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.apache.flink.annotation.Internal;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.concurrent.atomic.AtomicLong;

/** Utils for rewriters. */
@Internal
public class CodeSplitUtil {

    private static final AtomicLong COUNTER = new AtomicLong(0L);

    public static AtomicLong getCounter() {
        return COUNTER;
    }

    public static String newName(String name) {
        return name + "$" + COUNTER.getAndIncrement();
    }

    public static String getContextString(ParserRuleContext ctx) {
        if (ctx == null) {
            return "";
        }

        CharStream cs = ctx.start.getInputStream();
        return cs.getText(new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    public static int getContextTextLength(ParserRuleContext ctx) {
        return getContextString(ctx).length();
    }
}
