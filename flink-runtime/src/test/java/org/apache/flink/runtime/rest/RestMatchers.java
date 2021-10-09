/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest;

import org.apache.flink.runtime.rest.handler.RestHandlerException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Hamcrest matchers for REST handlers. */
public class RestMatchers {

    public static <T> Matcher<CompletableFuture<T>> respondsWithError(
            HttpResponseStatus responseStatus) {
        return new ErrorResponseStatusCodeMatcher<>(responseStatus);
    }

    private static final class ErrorResponseStatusCodeMatcher<T>
            extends TypeSafeDiagnosingMatcher<CompletableFuture<T>> {

        private final HttpResponseStatus expectedErrorResponse;

        ErrorResponseStatusCodeMatcher(HttpResponseStatus expectedErrorResponse) {
            this.expectedErrorResponse = expectedErrorResponse;
        }

        @Override
        protected boolean matchesSafely(
                CompletableFuture<T> future, Description mismatchDescription) {
            try {
                future.get();

                mismatchDescription.appendText("The request succeeded");
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new Error("interrupted test");
            } catch (ExecutionException e) {
                if (e.getCause() != null) {
                    if (RestHandlerException.class.isAssignableFrom(e.getCause().getClass())) {
                        final RestHandlerException rhe = (RestHandlerException) e.getCause();
                        if (rhe.getHttpResponseStatus() == expectedErrorResponse) {
                            return true;
                        } else {
                            mismatchDescription.appendText(
                                    "Error response had different status code: "
                                            + rhe.getHttpResponseStatus());
                            return false;
                        }
                    }
                }

                mismatchDescription.appendText(
                        "Future completed with different exception: " + e.getCause());
                return false;
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("An error response with status code: " + expectedErrorResponse);
        }
    }
}
