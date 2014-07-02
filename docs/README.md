This README gives an overview of how to build and contribute to the
documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure
that you always have docs corresponding to your checked out version. The online
documentation at http://flink.incubator.apache.org/ is also generated from the
files found here.

# Requirements

We use Markdown to write and Jekyll to translate the documentation to static
HTML. You can install all needed software via:

    gem install jekyll
    gem install redcarpet
    sudo easy_install Pygments

Redcarpet is needed for Markdown processing and the Python based Pygments is
used for syntax highlighting.

# Build

The `docs/build_docs.sh` script calls Jekyll and generates the documentation to
`docs/target`. You can then point your browser to `docs/target/index.html` and
start reading.

If you call the script with the preview flag `build_docs.sh -p`, Jekyll will
start a web server at `localhost:4000` and continiously generate the docs.
This is useful to preview changes locally.

# Contribute

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible
to use the [GitHub flavored syntax](http://github.github.com/github-flavored-markdown)
and intermix plain html.

In addition to Markdown, every page contains a front matter, which specifies the
title of the page. This title is used as the top-level heading for the page.

    ---
    title: "Title of the Page"
    ---

Furthermore, you can access variables found in `docs/_config.yml` as follows:

    {{ site.FLINK_VERSION_STABLE }}

This will be replaced with the value of the variable when generating the docs.

All documents are structed with headings. From these heading, an page outline is
automatically generated for each page.

```
# Level-1 Heading
## Level-2 Heading
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```