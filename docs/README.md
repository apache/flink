This README gives an overview of how to build and contribute to the
documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure
that you always have docs corresponding to your checked out version. The online
documentation at http://flink.apache.org/ is also generated from the
files found here.

# Requirements

The dependencies are declared in the Gemfile in this directory. We use Markdown
to write and Jekyll to translate the documentation to static HTML. All required
dependencies are installed locally when you build the documentation through the
`build_docs.sh` script. If you want to install the software manually, use Ruby's
Bundler Gem to install all dependencies:

    gem install bundler
    bundle install

Note that in Ubuntu based systems, it may be necessary to install the `ruby-dev`
via apt to build native code.

# Build

The `docs/build_docs.sh` script installs dependencies locally, calls Jekyll, and
generates the documentation in `docs/content`. You can then point your browser
to `docs/content/index.html` and start reading.

If you call the script with the preview flag `build_docs.sh -p`, Jekyll will
start a web server at `localhost:4000` and watch the docs directory for
updates. Use this mode to preview changes locally.

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

    {{ site.FLINK_VERSION_SHORT }}

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
