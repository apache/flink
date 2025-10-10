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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.Public;

/**
 * Throwing an exception in AsyncOperate causes the task to fail by default. If you do not want the
 * task to fail, you can throw this RetriableAsyncOperateException. In this case, the current
 * checkpoint will fail, and the AsyncOperate logic will try again the next checkpoint.
 */
@Public
public class RetriableAsyncOperateException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetriableAsyncOperateException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetriableAsyncOperateException(String message) {
        super(message);
    }

    public RetriableAsyncOperateException(Throwable cause) {
        super(cause);
    }
}
