This README gives an overview of how to build and contribute to the documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure that you always
have docs corresponding to your checked out version. The online documentation at
https://flink.apache.org/ is also generated from the files found here.

# Requirements

### Build the documentation and serve it locally

The Flink documentation uses [Hugo](https://gohugo.io/getting-started/installing/) to generate HTML files.  More specifically, it uses the *extended version* of Hugo with Sass/SCSS support. 

To build the documentation, you can install Hugo locally or use a Docker image. 

Both methods require you to execute commands in the directory of this module (`docs/`). The built site is served at http://localhost:1313/.

#### Using Hugo Docker image:

The image tag pins the same Hugo version as `setup_hugo.sh`. An older Hugo can't build the docs.

```sh
$ git submodule update --init --recursive
$ ./setup_docs.sh
$ docker pull hugomods/hugo:exts-0.126.3
$ docker run -v $(pwd):/src -p 1313:1313 hugomods/hugo:exts-0.126.3 hugo server --buildDrafts --buildFuture --bind 0.0.0.0
```

#### Local Hugo installation:

Make sure you have installed [Hugo](https://gohugo.io/getting-started/installing/) on your system.

```sh
$ ./setup_hugo.sh
```

Then build the docs from source:

```sh
$ ./build_docs.sh
```

The shell `./build_docs.sh` will integrate external connector docs, referencing `setup_docs.sh#integrate_connector_docs`.
This process involves cloning the repo of some external connectors, which can be time-consuming and prone to network issues.
So, if the connector docs have been synced before, and you wish to skip this step, you can do so by adding the following arg:

```sh
$ ./build_docs.sh --skip-integrate-connector-docs
```

The site can be viewed at http://localhost:1313/

## Include externally hosted documentation

With the ongoing efforts to move Flink's connectors from this repository to individual, dedicated
repositories, this also requires the documentation to be hosted outside this repo. However, 
we still want to serve all documentation as a whole on the Flink documentation website.

Adding new externally hosted documentation requires the following steps to be taken:

1. (If necessary) Move the existing documentation to the new repository

2. In the Flink repository, edit the `docs/setup_docs.sh` file and add a reference to your now 
externally hosted documentation. The reference will look like `integrate_connector_docs <connector_name> <branch_or_tag>`.

Replace <connector_name> with the name of your connector, e.g., `elasticsearch` for `flink-connector-elasticsearch`.

## Generate configuration tables

Configuration descriptions are auto generated from code. To trigger the generation, you need to run a command in the project root (see [Configuration documentation](https://github.com/apache/flink/blob/master/flink-docs/README.md#configuration-documentation).)

The resulting html files will be written to `layouts/shortcodes/generated`.

# Contribute

## Markdown

The documentation pages are written in [Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible to use [GitHub flavored syntax](http://github.github.com/github-flavored-markdown) and intermix plain html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the page and the layout to use. The title is used as the top-level heading for the page. The default layout is `plain` (found in `_layouts`).

    ---
    title: "Title of the Page"
    ---
    
    ---
    title: "Title of the Page" <-- Title rendered in the side nave
    weight: 1 <-- Weight controls the ordering of pages in the side nav. 
    type: docs <-- required
    aliases:  <-- Alias to setup redirect from removed page to this one
      - /alias/to/removed/page.html
    ---

## Structure

### Page

#### Headings

All documents are structured with headings. From these headings, you can automatically generate a page table of contents (see below).

```
# Level-1 Heading  <- Used for the title of the page 
## Level-2 Heading <- Start with this one for content
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with level-2 headings and use level-3 headings for subsections, etc. Don't use a different ordering, because you don't like how a headline looks.

#### Table of Contents

Table of contents are added automatically to every page, based on heading levels 2 - 4. 
The ToC can be omitted by adding the following to the front matter of the page:

    ---
    bookToc: false
    ---

### ShortCodes 

Flink uses [shortcodes](https://gohugo.io/content-management/shortcodes/) to add custom functionality
to its documentation markdown. The following are available for use:  

#### Flink Artifact

    {{< artifact flink-table-api-scala withScalaVersion >}}

This will be replaced by the maven artifact for flink-table-api-scala that users should copy into their pom.xml file. It will render out to:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-scala_2.12</artifactId>
    <version><!-- current flink version --></version>
</dependency>
```

It includes a number of optional flags:

* withScalaVersion: Includes the scala version suffix to the artifact id
* withTestScope: Includes `<scope>test</scope>` to the module. Useful for marking test dependencies.
* withTestClassifier: Includes `<classifier>tests</classifier>`. Useful when users should be pulling in Flink tests dependencies. This is mostly for the test harnesses and probably not what you want. 

You can also use the shortcodes (with same flags) instead:

* `artifact_gradle` to show the Gradle syntax
* `artifact_tabs` to create a tabbed view, showing both Maven and Gradle syntax

#### Flink Connector Artifact

    {{< connector_artifact flink-connector-elasticsearch 3.0.0 >}}

This will be replaced by the maven artifact for flink-connector-elasticsearch that users should copy into their pom.xml file. It will render out to:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch</artifactId>
    <version>3.0.0</version>
</dependency>
```

#### Back to Top

	{{< top >}}

This will be replaced by a back to top link. It is recommended to use these links at least at the end of each level-2 section.

#### Info Hints

	{{< hint info >}}
	Some interesting information
	{{< /hint >}}

The hint will be rendered in a blue box. This hint is useful when providing 
additional information for the user that does not fit into the flow of the documentation.

#### Info Warning 

    {{< hint warning >}}
    Something to watch out for. 
    {{< /hint >}}

The hint will be rendered in a yellow box. This hint is useful when highlighting
information users should watch out for to prevent errors. 

#### Info Danger

    {{< hint danger >}}
    Something to avoid
    {{< /hint >}}

The hint will be rendered in a red box. This hint is useful when highlighting
information users need to know to avoid data loss or to point out broken
functionality. 

#### Label

    {{< label "My Label" >}}

The label will be rendered in an inlined blue box. This is useful for labeling functionality
such as whether a SQL feature works for only batch or streaming execution. 

#### Flink version 

    {{< version >}}

Interpolates the current Flink version

#### Scala Version

    {{< scala_version >}}

Interpolates the default scala version

#### Stable

    {{< stable >}}
     Some content
    {{< /stable >}}

This shortcode will only render its content if the site is marked as stable. 

#### Unstable 

    {{< unstable >}}
    Some content 
    {{< /unstable >}}

This shortcode will only render its content if the site is marked as unstable. 

#### Query State Warning

    {{< query_state_warning >}}

Will render a warning the current SQL feature may have unbounded state requirements.

#### tab

    {{< tabs "sometab" >}}
    {{< tab "Java" >}}
    ```java
    System.out.println("Hello World!");
    ```
    {{< /tab >}}
    {{< tab "Scala" >}}
    ```scala
    println("Hello World!");
    ```
    {< /tab >}}
    {{< /tabs }}

Prints the content in tabs. IMPORTANT: The label in the outermost "tabs" shortcode must
be unique for the page. 

#### Github Repo

    {{< github_repo >}}

Renders a link to the apache flink repo. 

#### Github Link

    {{< gh_link file="/some/file.java" name="Some file" >}}

Renders a link to a file in the Apache Flink repo with a given name. 

#### JavaDocs Link
    {{< javadoc file="some/file" name="Some file" >}}

Renders a link to a file in the Apache Flink Java Documentation. 

#### PythonDocs Link
    {< pythondoc file="some/file" name="Some file" >}}

Renders a link to a file in the Apache Flink Python Documentation. 

#### FlinkDownloads Link

```
{{< downloads >}}
```

Renders a link to the apache flink download page. 

## Built-in function pages

Pages for built-in SQL functions live under `content/docs/sql/built-in-functions/<category>/`. They aren't written in Markdown. A content adapter generates each page, in English and Chinese, from one YAML file in `data/functions/`.

### Adding a function

1. Add `data/functions/<category>/<name>.yml`. `<category>` is the directory of the category under `content/docs/sql/built-in-functions/`, such as `regular-expressions`. `<name>` is the function name in lower case, such as `regexp_extract`, and the page is served at `<category>/regexp-extract/`. Use `data/functions/regular-expressions/regexp_extract.yml` as the reference for the fields.
2. Add a row for the function to its category in `data/sql_functions.yml` and `data/sql_functions_zh.yml`. Keep the rows in alphabetical order of the function name, the order of the pages in the sidebar. Give it `sql`, `table` and `page: regexp-extract`, without a `description`. The table shows the function's name, links it to the page and shows the page's summary.
3. Build the docs as described above and check the page in both languages. A broken `ref` fails the build, locally and in CI.

The Chinese page renders the same YAML, so the English text is its placeholder until someone translates it. If you aren't a native Chinese speaker, don't translate: add no Chinese file and leave the translation to native speakers. A translation goes in `data/functions_zh/<category>/<name>.yml`, and the fields it sets replace the English ones.

### What goes on a page

Every page has the same sections, in this order. A section is left out when the YAML has no field for it.

| Section     | What it says                                                                                                                                                  |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Summary     | One sentence on what the function returns. The category table shows it too.                                                                                   |
| See also    | Related functions, listed under the summary.                                                                                                                  |
| Syntax      | The SQL call, and the Java and Python Table API calls where they exist. There is no Scala tab, since the Scala API is deprecated.                             |
| Arguments   | For each argument: the accepted types in plain words, whether it is required, its default and what it means.                                                  |
| Returns     | What the function returns, then the cases that return NULL, then the return type and whether it is nullable.                                                  |
| Usage notes | What a user of the function needs and can't tell from the sections above, such as limits and determinism.                                                     |
| Examples    | A list of the scenarios, then each scenario with a title, one sentence, the SQL and its result. A Table API-only function shows Java and Python code instead. |

NULL handling has no section of its own. It goes into Returns, and the examples show it.

### What to leave out

A page tells a user how to use the function, not everything the code does. Leave out:

- Behavior that all functions share, such as that an untyped `NULL` needs a cast, or that arguments of the wrong type fail at planning time.
- Internal behavior, such as how the planner rewrites a call or how its code is generated.
- Exact error messages. Say when a call fails instead, such as "Not supported in streaming mode."
- Types and inputs that few users would try. Name the accepted types in plain words, as other engines do, such as "Any comparable type".
- Common sense, such as that a call needs its parentheses.
- Bugs. A page describes the intended behavior, and a bug goes to Jira.

In the YAML, quote a value only where YAML needs it: when it starts with a backtick, ends with a colon, or contains `: ` or ` #`.

### Accuracy

- Check every statement against the code rather than against earlier docs: the type strategies of the function in `BuiltInFunctionDefinitions`, its runtime implementation and its tests.
- Every result in an example comes from running its SQL, including NULL results.
