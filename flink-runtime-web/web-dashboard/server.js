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

// ----------------------------------------------------------------------------
//  This class implements a standalone proxy server, to simplify the
//  development of the web dashboard. See the "flink-runtime-web/README.md"
//  for details on how to use it.
// ----------------------------------------------------------------------------


var Hapi = require('hapi');

var server = new Hapi.Server();
var remotes = [
  { port: 8080, path: 'web-server' },
  { port: 8081, path: 'job-server' },
  { port: 8081, path: 'new-server' }
]

server.connection({ port: 3000 });

remotes.forEach(function (remote) {
  server.route([
    {
      method: 'GET',
      path: '/' + remote.path + '/{params*}',
      handler: {
        proxy: {
          mapUri: function(request, callback) {
            var url = "http://localhost:" + remote.port + "/" + request.url.href.replace('/' + remote.path + '/', '');
            console.log(request.url.href, '->', url);
            callback(null, url);
          },
          passThrough: true,
          xforward: true
        }
      }
    }
  ]);
});

server.route({
  method: 'GET',
  path: '/{param*}',
  handler: {
    directory: {
      path: 'web',
      listing: true
    }
  }
});

server.start(function () {
  console.log('Server running at:', server.info.uri);
});

