This README gives an overview of how to build and contribute to the documentation of Apache Flink.

The documentation is included with the source of Apache Flink in order to ensure that you always
have docs corresponding to your checked out version. The online documentation at
https://flink.apache.org/ is also generated from the files found here.

# Requirements

The dependencies are declared in the Gemfile in this directory. We use Markdown
to write and Jekyll to translate the documentation to static HTML. All required
dependencies are installed locally when you build the documentation through the
`build_docs.sh` script. If you want to install the software manually, use Ruby's
Bundler Gem to install all dependencies:

    gem install bundler -v 1.16.1
    bundle install

Note that in Ubuntu based systems, it may be necessary to install the following
packages: `rubygems ruby-dev libssl-dev build-essential`.

# Using Dockerized Jekyll

We dockerized the jekyll environment above. If you have [docker](https://docs.docker.com/),
you can run the following command to start the container.

```
cd flink/docs/docker
./run.sh
```

It takes a few moments to build the image for the first time but will be a second from the second time.
The run.sh command brings you in a bash session where you run the `./build_docs.sh` script mentioned above.


# Build

The `docs/build_docs.sh` script installs dependencies locally, calls Jekyll, and
generates the documentation in `docs/content`. You can then point your browser
to `docs/content/index.html` and start reading.

If you call the script with the preview flag `build_docs.sh -p`, Jekyll will
start a web server at `localhost:4000` and watch the docs directory for
updates. Use this mode to preview changes locally. 

You can call the script with the incremental flag `build_docs.sh -i`.
Jekyll will then serve a live preview at `localhost:4000`,
and it will be much faster because it will only rebuild the pages corresponding
to files that are modified. Note that if you are making changes that affect
the sidebar navigation, you'll have to build the entire site to see
those changes reflected on every page.

| Flag | Action | 
| -----| -------| 
| -p   | Run interactive preview | 
| -i   | Incremental builds | 
| -e   | Build only English docs |
| -z   | Build only Chinese docs |

## Generate configuration tables

Configuration descriptions are auto generated from code. To trigger the generation you need to run:

```
mvn -Pgenerate-config-docs install
```

The resulting html files will be written to `_includes/generated`. Tables are regenerated each time the command is invoked.
These tables can be directly included into the documentation:

```
{% include generated/file_name.html %}
```

# Contribute

## Markdown

The documentation pages are written in [Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible to use [GitHub flavored syntax](http://github.github.com/github-flavored-markdown) and intermix plain html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the page and the layout to use. The title is used as the top-level heading for the page. The default layout is `plain` (found in `_layouts`).

    ---
    title: "Title of the Page"
    ---

Furthermore, you can access the variables found in `docs/_config.yml` as follows:

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

This will be replaced by a back to top link. It is recommended to use these links at least at the end of each level-2 section.

#### Labels

	{% info %}
	{% warn %}

These will be replaced by an info or warning label. You can change the text of the label by providing an argument:

    {% info Recommendation %}

### Documentation

#### Navigation

The navigation on the left side of the docs is automatically generated when building the docs. You can modify the markup in `_include/sidenav.html`.

The structure of the navigation is determined by the front matter of all pages. The fields used to determine the structure are:

- `nav-id` => ID of this page. Other pages can use this ID as their parent ID.
- `nav-parent_id` => ID of the parent. This page will be listed under the page with id `nav-parent_id`.

Level 0 is made up of all pages, which have nav-parent_id set to `root`. There is no limitation on how many levels you can nest.

The `title` of the page is used as the default link text. You can override this via `nav-title`. The relative position per navigational level is determined by `nav-pos`.

If you have a page with sub pages, the link target will be used to expand the sub level navigation. If you want to actually add a link to the page as well, you can add the `nav-show_overview: true` field to the front matter. This will then add an `Overview` sub page to the expanded list.

The nesting is also used for the breadcrumbs like `Application Development > Libraries > Machine Learning > Optimization`.
