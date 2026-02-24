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

# PyFlink Documentation

This directory contains the Sphinx-based Python documentation for Apache Flink (PyDocs). It includes API reference documentation (auto-generated from docstrings) and a user guide with tutorials and guides for PyFlink users.

The built documentation is published at:
- **English:** `https://nightlies.apache.org/flink/flink-docs-{version}/api/python/`
- **Chinese:** `https://nightlies.apache.org/flink/flink-docs-{version}/api/python/zh/`

## Prerequisites

The docs directory has its own `pyproject.toml` with all Sphinx dependencies. You can install them with either [uv](https://docs.astral.sh/uv/) (recommended) or pip.

### Using uv (recommended)

```bash
cd flink-python/docs
uv sync
```

That's it. The Makefile auto-detects uv and will use `uv run` to invoke Sphinx.

### Using pip

```bash
cd flink-python/docs
pip install -r <(uv pip compile pyproject.toml)
```

Or install the packages listed in `docs/pyproject.toml` manually into your environment.

## Building the Documentation

All commands should be run from the `flink-python/docs/` directory.

### English only

```bash
make html
```

Output: `_build/html/` — open `_build/html/index.html` in a browser.

### English + Chinese

```bash
make html-zh
```

This builds English into `_build/html/` and Chinese into `_build/html/zh/`. A language toggle in the navbar allows switching between them.

### Chinese only

If the English docs are already built (e.g., in CI where `lint-python.sh` builds them first):

```bash
make zh
```

This runs `gettext` → `sphinx-intl update` → Chinese HTML build into `_build/html/zh/`.

### Serving locally

```bash
make serve
```

Then open http://localhost:8080/.

## Directory Structure

```
flink-python/docs/
├── conf.py                  # Sphinx configuration
├── index.rst                # Root toctree
├── Makefile                 # Build targets
├── _static/                 # Custom CSS/JS
├── _templates/              # Custom Jinja2 templates (e.g., language switcher)
├── reference/               # Auto-generated API reference (from docstrings)
├── examples/                # Code examples
├── user_guide/              # Guides and tutorials
│   ├── index.rst
│   ├── table/               # Table API guides
│   ├── datastream/          # DataStream API guides
│   └── *.rst                # General guides (config, debugging, etc.)
└── locales/                 # Translation files (see below)
    └── zh/
        └── LC_MESSAGES/
            └── user_guide/
                └── *.po     # Chinese translations
```

## Cross-References to Main Flink Docs

Links to pages in the main Flink documentation (the Hugo site) use the `:flinkdoc:` role, powered by Sphinx's `extlinks` extension:

```rst
See the :flinkdoc:`CLI documentation <docs/deployment/cli/>` for details.
```

This renders as a link to `https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/` by default.

The base URL is controlled by the `FLINK_DOCS_BASE_URL` environment variable:

```bash
# For a specific release:
export FLINK_DOCS_BASE_URL="https://nightlies.apache.org/flink/flink-docs-release-2.2"
make html
```

In CI, this is set automatically from the git branch name (see `.github/workflows/docs.sh`). For local builds, it defaults to `flink-docs-master`.

## Translations (Chinese)

Chinese translations use Sphinx's built-in [internationalization](https://www.sphinx-doc.org/en/master/usage/advanced/intl.html) system based on GNU gettext. Here's how it works:

### How it works

1. **`.pot` files** (PO Templates) — Auto-generated from the English `.rst` sources. Each paragraph becomes a translatable string (`msgid`). These are build artifacts in `_build/gettext/` and should never be edited manually.

2. **`.po` files** (Portable Object) — One per `.rst` file per language, stored in `locales/zh/LC_MESSAGES/`. Each file contains `msgid`/`msgstr` pairs where `msgid` is the English source text and `msgstr` is the Chinese translation. **These are the files you edit.**

3. **`.mo` files** (Machine Object) — Compiled binary versions of `.po` files, generated automatically during the build. These are gitignored.

### Updating an existing translation

Edit the relevant `.po` file in `locales/zh/LC_MESSAGES/user_guide/`. For example, to update the Chinese translation of the debugging guide:

```bash
# Edit the .po file
vi locales/zh/LC_MESSAGES/user_guide/debugging.po
```

Each entry looks like:

```po
#: ../../user_guide/debugging.rst:10
msgid "Logging"
msgstr "日志"
```

- `msgid` — The English source text (do not modify)
- `msgstr` — The Chinese translation (this is what you edit)
- Empty `msgstr ""` means untranslated (the English text will be shown)

After editing, rebuild the Chinese docs:

```bash
make zh
```

### Adding translations for a new page

When a new `.rst` page is added to `user_guide/`, the translation workflow is:

```bash
# 1. Regenerate .pot templates from the updated English sources
make gettext

# 2. Create/update .po files for Chinese
sphinx-intl update -p _build/gettext -l zh

# 3. Edit the new .po file in locales/zh/LC_MESSAGES/user_guide/
#    Fill in msgstr values with Chinese translations

# 4. Build to verify
make html-zh
```

### Syncing after English content changes

When the English `.rst` source is updated, the `.po` files need to be synced:

```bash
make gettext
sphinx-intl update -p _build/gettext -l zh
```

This preserves existing translations and marks changed entries as `#, fuzzy`. Fuzzy entries use the old translation as a suggestion but need review. Remove the `#, fuzzy` marker after verifying the translation is still correct.

### RST markup in translations

Translations must preserve RST inline markup exactly. Common patterns:

```po
# Inline code
msgid "Use ``TableEnvironment`` to create tables."
msgstr "使用 ``TableEnvironment`` 创建表。"

# Bold
msgid "This is **important**."
msgstr "这是 **重要的**。"

# Links
msgid "See `Apache Flink <https://flink.apache.org>`_ for details."
msgstr "详情请参阅 `Apache Flink <https://flink.apache.org>`_ 。"
```

Note: RST requires a space before and after inline markup (`` `` ``, `**`, etc.). This includes before Chinese punctuation — `` ``code`` 。`` not `` ``code``。``.

## Adding a New User Guide Page

1. Create the `.rst` file in the appropriate subdirectory:

   ```bash
   vi user_guide/my_new_page.rst
   ```

   Use an existing page as a template for the license header and heading style.

2. Add the page to the parent `index.rst` toctree:

   ```rst
   .. toctree::
       :maxdepth: 2

       existing_page
       my_new_page        <-- add here
   ```

3. Generate the translation stub:

   ```bash
   make gettext
   sphinx-intl update -p _build/gettext -l zh
   ```

4. Optionally populate `locales/zh/LC_MESSAGES/user_guide/my_new_page.po` with Chinese translations.

5. Build and verify:

   ```bash
   make html-zh
   ```
