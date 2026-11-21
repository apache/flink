---
title: "VARIANT Paths"
type: docs
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

# VARIANT Paths

`VARIANT_GET` and `TRY_VARIANT_GET` take a path that names one place in a Variant. A path starts
with `$`, the whole value, followed by steps: `.name`, `['name']` or `["name"]` for a field, and
`[n]` for an array element. For example, on
`PARSE_JSON('{"user": {"name": "ada"}, "items": [{"qty": 2}]}')`:

| Path                | Points at                          |
|---------------------|------------------------------------|
| `$`                 | the whole value                    |
| `$.user.name`       | `"ada"`                            |
| `$['user']["name"]` | `"ada"`                            |
| `$.items[0].qty`    | `2`                                |
| `$.items[1]`        | nothing, the index is past the end |
| `$.user[0]`         | nothing, `user` is not an array    |

- Array indices start at 0, as in `JSON_VALUE`, while `variant[INT]` starts at 1. So
  `VARIANT_GET(v, '$.items[0]')` and `v['items'][1]` are the same element.
- Use brackets with single or double quotes for a name with whitespace or one of the characters
  `. [ ] ( ) * ' " \`, for example `$['first name']` or `$["first name"]`. Inside the quotes, `\\`,
  `\'` and `\"` stand for a backslash and a quote.
- Wildcards, filters, slices, negative indices and the `lax` and `strict` modes are not supported,
  so a path always names one place.
- Trailing spaces are ignored.
- A malformed path literal fails at planning time. Any other malformed path fails the job in
  `VARIANT_GET` and returns NULL in `TRY_VARIANT_GET`.

A path points at nothing if a field is missing, an index is past the end, or a step does not match
the kind of value, such as a field of an array. Each function describes what it does in that case.
