<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Flink Web Dashboard

The web dashboard is work in progress towards the new Flink runtime monitor. In particular, it will
provide the following missing features of the current web dashboard:

 - Live progress monitoring (via live accumulators)
 - A graph view of the program, as it is executed.
 - A REST style API to access the status of individual jobs.
 - A more modular design

The dashboard listens at `http://localhost:8081`.

The new web dashboard is work in progress. It starts an HTTP server (by default at port 8081)
that serves the new web pages and additional background requests.

## Server Backend

The server side of the dashboard is implemented using [Netty](http://netty.io) with
[Netty Router](https://github.com/sinetja/netty-router) for REST paths.
The framework has very lightweight dependencies.

The code is regular Java code built via Maven. To add additional request handlers, follow the
example of the `org.apache.flink.runtime.webmonitor.handlers.JobSummaryHandler`.


## Dashboard Frontend 

The web dashboard is implemented using *angular.js*. The dashboard build infrastructure uses *node.js*.
The dashboard files are all pre-built, so one can try it out without building it.


### Preparing the Build Environment

Depending on your version of Linux, Windows or MacOS, you may need to manually install *node.js*
and *bower*.


#### Ubuntu Linux

Install *node.js* by following [these instructions](https://github.com/joyent/node/wiki/installing-node.js-via-package-manager).

Verify that the installed version is at least *2.11.3*, via `npm -version`.

Install *bower* via:

```
sudo npm install -g bower
```

Verify that the installed version is at least *1.4.1*, via `bower -version`.


Install *gulp* via:

```
sudo npm install -g gulp
```

Verify that the installed version is at least *3.9.0*, via `gulp -version`.


#### MacOS

First install *brew* by following [these instructions](http://brew.sh/).

Install *node.js* via:

```
brew install node
```

Install *bower* via:

```
sudo npm install -g bower
```

Verify that the installed version is at least *1.4.1*, via `bower -version`.

Install *gulp* via:

```
sudo npm install -g gulp
```

Verify that the installed version is at least *3.9.0*, via `gulp -version`.


### Building

The build process downloads all requires libraries via the *node.js* package management tool (*npm*)
and the *bower* dependency management tool. The final build tool is *gulp*.

```
cd flink-runtime-web/web-dashboard
npm install
bower install
gulp
```

The dashboard code is under `/app`. The result of the build process is under `/web`.

### Developing

When developing the dashboard, every change needs to recompile the files and update the server:

```
cd flink-runtime-web/web-dashboard
gulp
cd ../../flink-dist
mvn -DskipTests clean package
```

To simplify continuous development, one can use a *standalone proxy server*, together with automatic
re-compilation:

1. Edit the file `app/scripts/index.coffee`. Comment/uncomment the lines that define the `jobServer` URL.
2. Re-compile the files via `gulp`. By calling `gulp watch`, the build-tool autocompiles future changes.
3. Start the proxy server via `node server.js`
4. Access the dashboard at [`http://localhost:3000`](http://localhost:3000)
