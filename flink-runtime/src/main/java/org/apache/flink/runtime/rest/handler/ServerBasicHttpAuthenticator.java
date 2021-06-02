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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.commons.codec.digest.Md5Crypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** Netty handler for basic authentication on the Server side. */
@ChannelHandler.Sharable
public class ServerBasicHttpAuthenticator extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ServerBasicHttpAuthenticator.class);

    private final Map<String, String> responseHeaders;

    private final ImmutableMap<String, String> credentials;

    public ServerBasicHttpAuthenticator(
            ImmutableMap<String, String> credentials, final Map<String, String> responseHeaders) {
        this.responseHeaders = new HashMap<>(requireNonNull(responseHeaders));
        this.responseHeaders.put("WWW-Authenticate", "Basic realm=\"flink\"");
        this.credentials = credentials;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            HttpHeaders headers = ((HttpRequest) msg).headers();
            /*
             * look for auth token
             */
            String auth = headers.get("Authorization");
            if (auth == null) {
                HandlerUtils.sendErrorResponse(
                        ctx,
                        false,
                        new ErrorResponseBody("Missing Authorization header"),
                        HttpResponseStatus.UNAUTHORIZED,
                        responseHeaders);
                return;
            }
            int sp = auth.indexOf(' ');
            if (sp == -1 || !auth.substring(0, sp).equals("Basic")) {
                HandlerUtils.sendErrorResponse(
                        ctx,
                        false,
                        new ErrorResponseBody("Unknown Authorization method"),
                        HttpResponseStatus.UNAUTHORIZED,
                        responseHeaders);
                return;
            }
            byte[] b = Base64.getDecoder().decode(auth.substring(sp + 1));
            String userpass = new String(b);
            int colon = userpass.indexOf(':');
            if (colon < 0) {
                HandlerUtils.sendErrorResponse(
                        ctx,
                        false,
                        new ErrorResponseBody("No password found in basic authentication header"),
                        HttpResponseStatus.UNAUTHORIZED,
                        responseHeaders);
                return;
            }
            String uname = userpass.substring(0, colon);
            String pass = userpass.substring(colon + 1);

            if (checkCredentials(uname, pass)) {
                ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
            } else {
                HandlerUtils.sendErrorResponse(
                        ctx,
                        false,
                        new ErrorResponseBody("Invalid credentials"),
                        HttpResponseStatus.UNAUTHORIZED,
                        responseHeaders);
            }
        } else {
            // Only HttpRequests are authenticated
            ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
        }
    }

    /**
     * Checks credentials against the stored hashes using salted MD5 hashing as described in
     * http://httpd.apache.org/docs/2.2/misc/password_encryptions.html. This is the default format
     * produced by the htpasswd command.
     *
     * @param username Username provided in the http request
     * @param password Password provided in the http request
     * @return True if username & password match the stored credentials
     */
    private boolean checkCredentials(String username, String password) {
        String storedPwdHash = credentials.get(username);
        if (storedPwdHash == null) {
            return false;
        }

        String computedPwdHash =
                Md5Crypt.apr1Crypt(password.getBytes(), storedPwdHash.substring(6, 14));
        return storedPwdHash.equals(computedPwdHash);
    }
}
