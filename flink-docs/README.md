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

# Documentation generators

This module contains generators that create HTML files directly from Flink's source code.

## REST API documentation

The `RestAPIDocGenerator` can be used to generate a full reference of the REST API of a `RestServerEndpoint`. A separate file is generated for each endpoint.

To integrate a new endpoint into the generator
1. Add a new `DocumentingRestEndpoint` class to `RestAPIDocGenerator` that extends the new endpoint class
2. Add another call to `createHtmlFile` in `RestAPIDocGenerator#main`
3. Regenerate the documentation by running `mvn package -Dgenerate-rest-docs -nsu`
4. Integrate the generated file into the REST API documentation by adding `{% include generated/<file-name>.html %}` to the corresponding markdown file.

The documentation must be regenerated whenever
* a handler is added to/removed from a `RestServerEndpoint`
* any used `MessageHeaders` class or any referenced `RequestBody`, `ResponseBody`, `MessageParameters` or `MessageParameter` class is modified.