---
title: Security Updates
weight: 6
type: docs
aliases:
---
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

# Security Updates

This section lists fixed vulnerabilities in Flink.

<table class="table">
	<thead>
		<tr>
			<th style="width: 20%">CVE ID</th>
			<th style="width: 30%">Affected Flink versions</th>
			<th style="width: 50%">Notes</th>
		</tr>
	</thead>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-1960">CVE-2020-1960</a>
		</td>
		<td>
			1.1.0 to 1.1.5, 1.2.0 to 1.2.1, 1.3.0 to 1.3.3, 1.4.0 to 1.4.2, 1.5.0 to 1.5.6, 1.6.0 to 1.6.4, 1.7.0 to 1.7.2, 1.8.0 to 1.8.3, 1.9.0 to 1.9.2, 1.10.0
		</td>
		<td>
			Users are advised to upgrade to Flink 1.9.3 or 1.10.1 or later versions or remove the port parameter from the reporter configuration (see advisory for details).
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-17518">CVE-2020-17518</a>
		</td>
		<td>
			1.5.1 to 1.11.2
		</td>
		<td>
			<a href="https://github.com/apache/flink/commit/a5264a6f41524afe8ceadf1d8ddc8c80f323ebc4">Fixed in commit a5264a6f41524afe8ceadf1d8ddc8c80f323ebc4</a> <br>
			Users are advised to upgrade to Flink 1.11.3 or 1.12.0 or later versions.
		</td>
	</tr>
	<tr>
		<td>
			<a href="https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-17519">CVE-2020-17519</a>
		</td>
		<td>
			1.11.0, 1.11.1, 1.11.2
		</td>
		<td>
			<a href="https://github.com/apache/flink/commit/b561010b0ee741543c3953306037f00d7a9f0801">Fixed in commit b561010b0ee741543c3953306037f00d7a9f0801</a> <br>
			Users are advised to upgrade to Flink 1.11.3 or 1.12.0 or later versions.
		</td>
	</tr>
</table>
