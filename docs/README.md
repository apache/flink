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

```sh
$ git submodule update --init --recursive
$ ./setup_docs.sh
$ docker pull jakejarvis/hugo-extended:latest
$ docker run -v $(pwd):/src -p 1313:1313 jakejarvis/hugo-extended:latest server --buildDrafts --buildFuture --bind 0.0.0.0
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

Configuration descriptions are auto generated from code. To trigger the generation you need to run in the project root:

```
mvn -Pgenerate-config-docs install -Dfast -DskipTests
```

The resulting html files will be written to `layouts/shortcodes/generated`. Tables are regenerated each time the command is invoked.
These tables can be directly included into the documentation:

```
{{< generated/file_name >}}
```

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

    {{< artifact flink-streaming-scala withScalaVersion >}}

This will be replaced by the maven artifact for flink-streaming-scala that users should copy into their pom.xml file. It will render out to:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.12</artifactId>
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
