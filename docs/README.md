This README gives an overview of how to build and contribute to the documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure that you always
have docs corresponding to your checked out version. The online documentation at
http://flink.apache.org/ is also generated from the files found here.

# Requirements

We use Markdown to write and Jekyll to translate the documentation to static HTML. You can install
all needed software via:

    gem install jekyll
    gem install kramdown
    sudo easy_install Pygments

Kramdown is needed for Markdown processing and the Python based Pygments is used for syntax
highlighting.

# Build

The `docs/_build_docs.sh` script calls Jekyll and generates the documentation in `docs/target`. You
can then point your browser to `docs/target/index.html` and start reading.

If you call the script with the preview flag `_build_docs.sh -p`, Jekyll will start a web server at
`localhost:4000` and watch the docs directory for updates. Use this mode to preview changes locally.

# Contribute

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible to use the
[GitHub flavored syntax](http://github.github.com/github-flavored-markdown) and intermix plain html.

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the
page and the layout to use. The title is used as the top-level heading for the page.

    ---
    title: "Title of the Page"
    ---

Furthermore, you can access variables found in `docs/_config.yml` as follows:

    {{ site.NAME }}

This will be replaced with the value of the variable called `NAME` when generating
the docs.

All documents are structed with headings. From these heading, a page outline is
automatically generated for each page.

```
# Level-1 Heading  <- Used for the title of the page
## Level-2 Heading <- Start with this one
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with level-2 headings and
use level-3 headings for subsections, etc. Don't use a different ordering, because you don't like
how a headline looks.
