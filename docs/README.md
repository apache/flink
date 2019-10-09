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
you can run following command to start the container.

```
cd flink/docs/docker
./run.sh
```

It takes a few moment to build the image for the first time, but will be a second from the second time.
The run.sh command brings you in a bash session where you run the `./build_docs.sh` script mentioned above.


# Build

The `docs/build_docs.sh` script installs dependencies locally, calls Jekyll, and
generates the documentation in `docs/content`. You can then point your browser
to `docs/content/index.html` and start reading.

If you call the script with the preview flag `build_docs.sh -p`, Jekyll will
start a web server at `localhost:4000` and watch the docs directory for
updates. Use this mode to preview changes locally. 

If you have ruby 2.0 or greater, 
you can call the script with the incremental flag `build_docs.sh -i`.
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

#### Navigation

The navigation on the left side of the docs is automatically generated when building the docs. You can modify the markup in `_include/sidenav.html`.

The structure of the navigation is determined by the front matter of all pages. The fields used to determine the structure are:

- `nav-id` => ID of this page. Other pages can use this ID as their parent ID.
- `nav-parent_id` => ID of the parent. This page will be listed under the page with id `nav-parent_id`.

Level 0 is made up of all pages, which have nav-parent_id set to `root`. There is no limitation on how many levels you can nest.

The `title` of the page is used as the default link text. You can override this via `nav-title`. The relative position per navigational level is determined by `nav-pos`.

If you have a page with sub pages, the link target will be used to expand the sub level navigation. If you want to actually add a link to the page as well, you can add the `nav-show_overview: true` field to the front matter. This will then add an `Overview` sub page to the expanded list.

The nesting is also used for the breadcrumbs like `Application Development > Libraries > Machine Learning > Optimization`.




     Apache License
                           Version 2.0, January 2004
                        https://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright 2019 Rolando Gopez Lacuata 

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
