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
