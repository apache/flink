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

package org.apache.flink.connector.testframe.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Test utils. */
public class TestUtils {
    public static void timeoutAssert(
            ExecutorService executorService, Runnable task, long time, TimeUnit timeUnit) {
        Future future = executorService.submit(task);
        try {
            future.get(time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException("Test failed to get the result.", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Test failed with some exception.", e);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    String.format("Test timeout after %d %s.", time, timeUnit.name()), e);
        } finally {
            future.cancel(true);
        }
    }
}
