---
title: "SQL Connectors download page"
nav-title: Download
nav-parent_id: sql-connectors
nav-pos: 99
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

The page contains links to optional sql-client connectors and formats that are not part of the
 binary distribution.
 
{% if site.is_stable %}

# Optional SQL formats
-------------------

<table class="table table-bordered">
    <thead>
    <tr>
        <th style="text-align: left">Name</th>
        <th style="text-align: left">Download link</th>
    </tr>
    </thead>
    <tbody>
{% for entry in site.data.sql-connectors %}
    {% for connector in entry %}
      {% if connector.category == "format" and connector.built-in != true and connector.sql-url != nil %}
        {% assign url = connector.sql-url | liquify %}
        <tr>
            <td style="text-align: left">{{connector.name}}</td>
            <td style="text-align: left"><a href="{{ url }}">Download</a> (<a href="{{ url }}.asc">asc</a>, <a href="{{ url }}.sha1">sha1</a>) </td>
        </tr>
      {% endif %}
    {% endfor %}
{% endfor %}
    </tbody>
</table>

# Optional SQL connectors
-------------------  

<table class="table table-bordered">
    <thead>
    <tr>
        <th style="text-align: left">Name</th>
        <th style="text-align: left">Versions</th>
        <th style="text-align: left">Download link</th>
    </tr>
    </thead>
    <tbody>
{% for entry in site.data.sql-connectors %}
    {% for connector in entry %}
      {% if connector.category == "connector" and connector.built-in != true %}
        {% if connector.versions != nil %}
            {% assign row0=connector.versions[0] %}
            <tr>
                <td style="text-align: left" rowspan="{{connector.versions | size}}">{{connector.name}}</td>
                <td style="text-align: left">{{row0.version}}</td>
                {% assign url = row0.sql-url | liquify %}
                <td style="text-align: left"><a href="{{ url }}">Download</a> (<a href="{{ url }}.asc">asc</a>, <a href="{{ url }}.sha1">sha1</a>) </td>
            </tr>
            {% for version in connector.versions offset:1 %}
                <tr>
                    <td style="text-align: left">{{version.version}}</td>
                    {% assign url = version.sql-url | liquify %}
                    <td style="text-align: left"><a href="{{ url }}">Download</a> (<a href="{{ url }}.asc">asc</a>, <a href="{{ url }}.sha1">sha1</a>) </td>
                </tr>
            {% endfor %}
        {% elsif connector.sql-url != nil %}
            <tr>
                <td style="text-align: left">{{connector.name}}</td>
                <td style="text-align: left"></td>
                {% assign url = connector.sql-url | liquify %}
                <td style="text-align: left"><a href="{{ url }}">Download</a> (<a href="{{ url }}.asc">asc</a>, <a href="{{ url }}.sha1">sha1</a>) </td>
            </tr>
        {% endif %}
      {% endif %}
    {% endfor %}
{% endfor %}
    </tbody>
</table>

{% else %}
<p style="border-radius: 5px; padding: 5px" class="bg-info">
  <b>Note</b>: The links are available only for stable releases.
</p>
{% endif %}
