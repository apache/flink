---
title: "Variant"
weight: 17
type: docs
sqlFunctions: variant
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

# Variant Functions

Variant functions parse JSON and XML text into [`VARIANT`]({{< ref "docs/sql/reference/data-types" >}}#variant) values and read values out of them, such as `PARSE_JSON`, `PARSE_XML` and `VARIANT_GET`. To turn a `VARIANT` back into JSON text, use [`JSON_STRING`]({{< ref "docs/sql/built-in-functions/json" >}}).

{{< sql_functions "variant" >}}

## XML to VARIANT

The `PARSE_XML` function maps the root element of an XML document to a `VARIANT` object. For
example:

```sql
-- {"book":{"#":{"price":[1,2],"title":0},"@pages":"320","price":["12.50","13.50"],"title":"Dune"}}
PARSE_XML('<book pages="320"><title>Dune</title><price>12.50</price><price>13.50</price></book>')
```

Attributes become fields with an `@` prefix, and a child element that occurs more than once becomes
an array. The `#` field records the order of the children, as described below. It can be ignored
when reading fields.

Values are strings, unless the document declares a type with `xsi:type`, see below. Since a cast
from a `VARIANT` never parses a string, cast a value to `STRING` first and convert it with a regular
cast:

```sql
CAST(v['book']['title'] AS STRING)                -- 'Dune'
CAST(v['book']['price'][2] AS STRING)             -- '13.50'
CAST(CAST(v['book']['@pages'] AS STRING) AS INT)  -- 320
```

Each element is mapped by the following rules:

| XML input                                                          | Stored `VARIANT` value                                      |
|--------------------------------------------------------------------|-------------------------------------------------------------|
| Element with `xsi:nil="true"`                                      | `null`, or field `$` with `null` if it has other attributes |
| Element without attributes and child elements                      | its text, or `""` if it is empty                            |
| Any other element                                                  | object                                                      |
| Attribute `name`                                                   | field `@name`, a `STRING`                                   |
| Child element `name`                                               | field `name`                                                |
| Text of an element with attributes or child elements               | field `$`                                                   |
| Child element or text that occurs more than once                   | array, in document order                                    |
| Element with differently named children, or with children and text | field `#` with the order of its content                     |

A `VARIANT` object doesn't keep the order of its fields, so the `#` field records it. It maps each
key to the position of its item, or to an array of positions if the key occurs more than once.
Positions count the child elements and texts of the element, starting at 0. For example:

```sql
-- {"book":{"#":{"$":[1,3],"price":[2,4],"title":0},
--          "$":["This is a description","Text in between"],
--          "@pages":"320",
--          "price":["12.50",{"$":"13.50","@currency":"EUR"}],
--          "title":"Dune"}}
PARSE_XML('
  <book pages="320">
    <title>Dune</title>
    This is a description
    <price>12.50</price>
    Text in between
    <price currency="EUR">13.50</price>
  </book>')
```

Here, `title` is the first item, the two texts are the second and the fourth, and the two `price`
elements are the third and the fifth. The second `price` has an attribute, so it is stored as an
object, while the first one is a string. An element whose content has only one key has no `#` field,
since the order of an array is the document order.

Whether a child element is stored as a single value or as an array depends on how often it occurs,
so two documents that follow the same schema can map to different shapes. The default behavior
ensures a space-efficient representation. With `PARSE_XML(xml, TRUE)`, every child element, text,
and position in a `#` field is stored as an array, even if it occurs only once. The root element is never wrapped, since a document has exactly one.

```sql
-- {"book":{"title":["Dune"]}}
PARSE_XML('<book><title>Dune</title></book>', TRUE)
```

An `xsi:type` attribute parses the text of an element as the matching kind, which replaces the text,
or the `$` field if the element has other attributes. The prefix of the type is ignored, so `xs:int`,
`xsd:int`, and `int` are the same type. If the element has child elements or no text, the type isn't
listed below, or the text isn't a valid value of the type, the text stays a `STRING` and `xsi:type`
is kept as an attribute. For example, `<a xsi:type="int">5</a>` maps to `{"a":5}`, and
`<a xsi:type="int">five</a>` maps to `{"a":{"$":"five","@xsi:type":"int"}}`. The text has to be in
the lexical form that XML Schema defines for the type, so `1e5` isn't a `decimal`, `1f` isn't a
`float`, and `12:30` isn't a `time`. Years after 9999 and the time `24:00:00` stay a `STRING` as well.
For a `dateTime` with an offset, this checks both the year in the text and the year in UTC. So
`10000-01-01T00:00:00+14:00` stays a `STRING`, and so does `9999-12-31T23:00:00-08:00`, which is
`10000-01-01 07:00:00` in UTC.

| `xsi:type`                   | Stored `VARIANT` kind                        |
|------------------------------|----------------------------------------------|
| `string`                     | `STRING`                                     |
| `boolean`                    | `BOOLEAN`, from `true`, `false`, `1`, or `0` |
| `byte`                       | `TINYINT`                                    |
| `short`                      | `SMALLINT`                                   |
| `int`                        | `INT`                                        |
| `long`                       | `BIGINT`                                     |
| `integer`, `decimal`         | `DECIMAL`, with at most 38 digits            |
| `float`                      | `FLOAT`, except for `NaN` and infinity       |
| `double`                     | `DOUBLE`, except for `NaN` and infinity      |
| `date` without an offset     | `DATE`                                       |
| `time` without an offset     | `TIME`                                       |
| `dateTime` with an offset    | `TIMESTAMP_LTZ`                              |
| `dateTime` without an offset | `TIMESTAMP`                                  |
| `base64Binary`               | `BYTES`                                      |

`float` and `double` keep the sign of zero, so `-0` maps to `-0.0`. The other numeric types have no
negative zero, so `-0` maps to `0`.

The offset of a `dateTime` is `Z` or a signed `hh:mm` of at most 14 hours. It is used to compute the
instant and isn't stored, since a `TIMESTAMP_LTZ` has no offset. For example,
`2026-09-30T12:30:00+02:00`, `2026-09-30T10:30:00Z`, and `2026-09-30T06:00:00-04:30` map to the same
`TIMESTAMP_LTZ`. A `date` or `time` with an offset stays a `STRING`, since it can't be converted to an
instant without a date or a time.

`TIME` keeps microseconds, so `12:30:45.1234567` is stored as `12:30:45.123456`. `TIMESTAMP` and
`TIMESTAMP_LTZ` keep nanoseconds if the value has digits below a microsecond and is in the
nanosecond range. A 64-bit count of nanoseconds since `1970-01-01 00:00:00` UTC covers
`1677-09-21 00:12:43.145224192` to `2262-04-11 23:47:16.854775807` UTC. A `dateTime` without an
offset is compared as if it were in UTC, so the range is the same in every session time zone.
Outside the range, the value keeps microseconds, so `2300-01-01T00:00:00.000000001` is stored as
`2300-01-01 00:00:00`. Further digits are truncated, like in a cast, instead of keeping the text as
a `STRING`.

A typed value casts directly to its type, without the cast to `STRING`:

```sql
CAST(PARSE_XML('<a xsi:type="int">5</a>')['a'] AS INT)                                  -- 5
CAST(PARSE_XML('<a xsi:type="dateTime">2026-09-30T12:30:00</a>')['a'] AS TIMESTAMP(3))  -- 2026-09-30 12:30:00.000
```

An `xsi:nil="true"` attribute takes precedence over the content of an element, so
`<price xsi:nil="true">12.50</price>` maps to `{"price":null}`. If the element has other attributes,
its content is replaced with a `null` text, so `<price xsi:nil="true" currency="EUR">12.50</price>`
maps to `{"price":{"$":null,"@currency":"EUR"}}`. In both cases, `xsi:nil` and `xsi:type` are dropped,
and so is `xsi:nil="false"`. Like in XML Schema, `xsi:nil` accepts `true`, `false`, `1`, and `0`,
and any other value is kept as a regular attribute.

`xsi:type` and `xsi:nil` are recognized by their literal `xsi:` prefix, even if the document doesn't
declare it or binds it to a different namespace. The declarations
`xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"` and
`xmlns:xs="http://www.w3.org/2001/XMLSchema"` are dropped, since the prefixes are recognized without
them. Declarations with other namespaces are kept, for example as `@xmlns:xsi`.

`PARSE_XML` also normalizes the document:

- Text is trimmed of spaces, tabs, and line breaks, and whitespace-only text between tags is dropped.
- Comments and processing instructions are dropped. CDATA sections are text.
- Entities and character references are expanded, including the entities that the document declares
  in its DTD.
- Names are kept as written, including their namespace prefix. Namespace declarations are regular
  attributes, for example `@xmlns:ns`.

A document fails to parse if it:

- references an external entity or an external DTD,
- expands more than 64,000 entities, or more than about 16 million characters of entity text,
- nests elements more than 500 levels deep,
- is an XML 1.1 document.

In all of these cases, `TRY_PARSE_XML` returns `NULL`.

## VARIANT to XML String

The `XML_STRING` function converts a `VARIANT` into an XML string. It reverses the mapping of
`PARSE_XML`, so `PARSE_XML(XML_STRING(v))` gives `v` for any `v` from `PARSE_XML` with the same
`force_array`. The other way around, `XML_STRING(PARSE_XML(xml))` gives the normalized document,
without the XML declaration, comments, and whitespace between tags, and with the attributes in the
order of their keys. For example:

```sql
-- '<book pages="320"><title>Dune</title>This is a description<price>12.50</price>Text in between<price currency="EUR">13.50</price></book>'
XML_STRING(PARSE_XML('
  <book pages="320">
    <title>Dune</title>
    This is a description
    <price>12.50</price>
    Text in between
    <price currency="EUR">13.50</price>
  </book>'))
```

`XML_STRING` writes a `VARIANT` by the following rules:

- `@` fields are written as attributes, `$` fields as text, and the other fields as child elements.
- The content of an element is written in the order of its `#` field. Content without a position in
  the `#` field follows in the order of its keys, so without a `#` field, all content is written in
  key order.
- Each item of an array is written as an element or a text, so an empty array writes nothing.
- An empty string is written as `<a></a>`. A `NULL`, and a `$` field with `NULL`, are written with
  `xsi:nil="true"`, for example `<a xsi:nil="true"/>` and `<a id="1" xsi:nil="true"/>`.
- A scalar other than a string gets the matching `xsi:type` from the table above with the `xs`
  prefix, for example `<a xsi:type="xs:int">5</a>`. A `DECIMAL` gets `xs:decimal`, and a
  `TIMESTAMP_LTZ` gets `xs:dateTime` and is written in UTC, for example `2026-09-30T10:30:00Z`.
  Like in `PARSE_XML`, only the text of an element without child elements is typed.
- `NaN` and infinity are written as `NaN`, `INF`, and `-INF`, like in XML Schema. `PARSE_XML` reads
  them back as strings, like dates and timestamps after the year 9999.
- XML Schema has no type for a `UUID`, so it is written as text without `xsi:type`, and reads back
  as a `STRING`.
- If the result uses the `xsi` or `xs` prefix, the root element declares it as
  `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"` or
  `xmlns:xs="http://www.w3.org/2001/XMLSchema"`, unless the `VARIANT` declares the prefix on the
  root element itself, for example with an `@xmlns:xs` field.
- `<`, `>`, and `&` are escaped, and so is `"` in attribute values. Tabs and line breaks in
  attribute values, and carriage returns in text, are written as character references such as
  `&#10;`, since an XML parser would read them back as spaces and line feeds.

`XML_STRING` fails if the `VARIANT` has no XML representation, which is the case if:

- it isn't an object with a single field for the root element, or the root element is an array,
- a key isn't an XML name that `PARSE_XML` accepts, for example `1a` or `a b`, apart from the `@`
  prefix of an attribute,
- a string has a character that [XML 1.0 doesn't allow](https://www.w3.org/TR/xml/#charsets), for
  example the control character `U+0000`,
- an array directly contains another array, for example `{"r":{"a":[["x"]]}}`,
- an `@` field holds an object, an array, or `NULL`,
- a `$` field holds an object,
- a `$` field with `NULL` isn't the only content of its element, or an element has an `@xsi:nil`
  field that is `true` and content,
- a `#` field isn't an object, doesn't have one integer position for each item of a key, or has a
  position more than once,
- an element would need two `xsi:type` or two `xsi:nil` attributes, for example
  `{"a":{"$":5,"@xsi:type":"T"}}`, where the `5` needs `xsi:type="xs:int"` as well.

## Variant paths

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
- A malformed path literal fails at planning time. Any other malformed path fails when the function
  runs.

A path points at nothing if a field is missing, an index is past the end, or a step does not match
the kind of value, such as a field of an array. Each function describes what it does in that case.
