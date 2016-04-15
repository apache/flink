This README gives an overview of how to build and contribute to the documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure that you always
have docs corresponding to your checked out version. The online documentation at
http://flink.apache.org/ is also generated from the files found here.

# Requirements

We use Markdown to write and Jekyll to translate the documentation to static HTML. Kramdown is
needed for Markdown processing and the Python based Pygments is used for syntax highlighting. To run
Javascript code from Ruby, you need to install a javascript runtime (e.g. `therubyracer`). You can
install all needed software via the following commands:

    gem install jekyll -v 2.5.3
    gem install kramdown -v 1.9.0
    gem install pygments.rb -v 0.6.3
    gem install therubyracer -v 0.12.2
    sudo easy_install Pygments

Note that in Ubuntu based systems, it may be necessary to install the `ruby-dev` and
`python-setuptools` packages via apt.

# Using Dockerized Jekyll

We dockerized the jekyll environment above. If you have [docker](https://docs.docker.com/),
you can run following command to start the container.

```
cd flink/docs/docker
./run.sh
```

It takes a few moment to build the image for the first time, but will be a second from the second time.
The run.sh command brings you in a bash session where you can run following doc commands.

# Build

The `docs/build_docs.sh` script calls Jekyll and generates the documentation in `docs/target`. You
can then point your browser to `docs/target/index.html` and start reading.

If you call the script with the preview flag `build_docs.sh -p`, Jekyll will start a web server at
`localhost:4000` and watch the docs directory for updates. Use this mode to preview changes locally.

# Contribute

## Markdown

The documentation pages are written in [Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible to use [GitHub flavored syntax](http://github.github.com/github-flavored-markdown) and intermix plain html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the page and the layout to use. The title is used as the top-level heading for the page. The default layout is `plain` (found in `_layouts`).

    ---
    title: "Title of the Page"
    ---

Furthermore, you can access variables found in `docs/_config.yml` as follows:

    {{ site.NAME }}

This will be replaced with the value of the variable called `NAME` when generating the docs.

## Structure

### Page

#### Headings

All documents are structured with headings. From these headings, you can automatically generate a page table of contents (see below).

```
# Level-1 Heading  <- Used for the title of the page (don't use this)
## Level-2 Heading <- Start with this one
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with level-2 headings and use level-3 headings for subsections, etc. Don't use a different ordering, because you don't like how a headline looks.

#### Table of Contents

    * This will be replaced by the TOC
    {:toc}


Add this markup (both lines) to the document in order to generate a table of contents for the page. Headings until level 3 headings are included.

You can exclude a heading from the table of contents:

    # Excluded heading
    {:.no_toc}

#### Back to Top

	{% top %}

This will be replaced by a default back to top link. It is recommended to use these links at least at the end of each level-2 section.

#### Labels

	{% info %}
	{% warn %}

These will be replaced by a info or warning label. You can change the text of the label by providing an argument:

    {% info Recommendation %}

### Documentation

#### Top Navigation

You can modify the top-level navigation in two places. You can either edit the `_includes/navbar.html` file or add tags to your page frontmatter (recommended).

    # Top-level navigation
    top-nav-group: apis
    top-nav-pos: 2
    top-nav-title: <strong>Batch Guide</strong> (DataSet API)

This adds the page to the group `apis` (via `top-nav-group`) at position `2` (via `top-nav-pos`). Furthermore, it specifies a custom title for the navigation via `top-nav-title`. If this field is missing, the regular page title (via `title`) will be used. If no position is specified, the element will be added to the end of the group. If no group is specified, the page will not show up.

Currently, there are groups `quickstart`, `setup`, `deployment`, `apis`, `libs`, and `internals`.

#### Sub Navigation

A sub navigation is shown if the field `sub-nav-group` is specified. A sub navigation groups all pages with the same `sub-nav-group`. Check out the streaming or batch guide as an example.

    # Sub-level navigation
    sub-nav-group: batch
    sub-nav-id: dataset_api
    sub-nav-pos: 1
    sub-nav-title: DataSet API

The fields work similar to their `top-nav-*` counterparts.

In addition, you can specify a hierarchy via `sub-nav-id` and `sub-nav-parent`:

    # Sub-level navigation
    sub-nav-group: batch
    sub-nav-parent: dataset_api
    sub-nav-pos: 1
    sub-nav-title: Transformations

This will show the `Transformations` page under the `DataSet API` page. The `sub-nav-parent` field has to have a matching `sub-nav-id`.

#### Breadcrumbs

Pages with sub navigations can use breadcrumbs like `Batch Guide > Libraries > Machine Learning > Optimization`.

The breadcrumbs for the last page are generated from the front matter. For the a sub navigation root to appear (like `Batch Guide` in the example above), you have to specify the `sub-nav-group-title`. This field designates a group page as the root.
