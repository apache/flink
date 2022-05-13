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

package org.apache.flink.runtime.rest.messages.job.savepoints.stop;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** These headers define the protocol for triggering a "stop" action. */
public class StopWithSavepointTriggerHeaders
        extends AsynchronousOperationTriggerMessageHeaders<
                StopWithSavepointRequestBody, SavepointTriggerMessageParameters> {

    private static final StopWithSavepointTriggerHeaders INSTANCE =
            new StopWithSavepointTriggerHeaders();

    private static final String URL = String.format("/jobs/:%s/stop", JobIDPathParameter.KEY);

    private StopWithSavepointTriggerHeaders() {}

    @Override
    public Class<StopWithSavepointRequestBody> getRequestClass() {
        return StopWithSavepointRequestBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public SavepointTriggerMessageParameters getUnresolvedMessageParameters() {
        return new SavepointTriggerMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        /*
        Note: this is different to the existing implementation for which the targetDirectory is a path parameter
        Having it as a path parameter has several downsides as it
        	- is optional (which we only allow for query parameters)
        	- causes parsing issues, since the path is not reliably treated as a single parameter
        	- does not denote a hierarchy which path parameters are supposed to do
        	- interacts badly with the POST spec, as it would require the progress url to also contain the targetDirectory
         */

        return URL;
    }

    public static StopWithSavepointTriggerHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    protected String getAsyncOperationDescription() {
        return "Stops a job with a savepoint. Optionally, it can also emit a MAX_WATERMARK before taking"
                + " the savepoint to flush out any state waiting for timers to fire.";
    }
}
