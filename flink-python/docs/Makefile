################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

ifndef SPHINXBUILD
ifndef SPHINXPYTHON
SPHINXBUILD   = sphinx-build
SPHINXPYTHON  = python
endif
endif

# You can set SPHINXBUILD to specify the sphinx-build or SPHINXPYTHON to specify the python env to call the sphinx module
# If SPHINXBUILD is set, the command is 'sphinx-build'.
# Elseif SPHINXPYTHON is set, the command is 'python -m sphinx'.
ifdef SPHINXBUILD
# Check whether sphinx-build is installed
ifeq ($(shell hash $(SPHINXBUILD) >/dev/null 2>&1; echo $$?), 1)
$(error The command '$(SPHINXBUILD)' was not found.If you don't have Sphinx installed, grab it from http://sphinx-doc.org/)
endif
else
ifeq ($(shell $(SPHINXPYTHON) -c 'import sphinx' >/dev/null 2>&1; echo $$?), 1)
# Check whether sphinx module is installed in $(SPHINXPYTHON)
$(error '$(SPHINXPYTHON)' doesn't install sphinx module.You can use specified pip of $(SPHINXPYTHON) to install sphinx)
endif
SPHINXBUILD = $(SPHINXPYTHON) -m sphinx
endif

# The project depends on py4j, so py4j-*-src.zip should be set in PYTHONPATH.
py4j_lib = $(shell echo ../lib/py4j-*-src.zip)
export PYTHONPATH = $(realpath $(py4j_lib))

# You can set these variables from the command line.
SPHINXOPTS   ?=
PAPER        ?=
SOURCEDIR    ?= .
BUILDDIR     ?= _build

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d _build/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: help clean html dirhtml pickle json htmlhelp qthelp latex changes linkcheck doctest

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html      to make standalone HTML files"
	@echo "  dirhtml   to make HTML files named index.html in directories"
	@echo "  pickle    to make pickle files"
	@echo "  json      to make JSON files"
	@echo "  htmlhelp  to make HTML files and a HTML help project"
	@echo "  qthelp    to make HTML files and a qthelp project"
	@echo "  latex     to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  changes   to make an overview of all changed/added/deprecated items"
	@echo "  linkcheck to check all external links for integrity"
	@echo "  doctest   to run all doctests embedded in the documentation (if enabled)"

clean:
	-rm -rf _build/*

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) _build/html
	@echo
	@echo "Build finished. The HTML pages are in _build/html."

dirhtml:
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) _build/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in _build/dirhtml."

pickle:
	$(SPHINXBUILD) -b pickle $(ALLSPHINXOPTS) _build/pickle
	@echo
	@echo "Build finished; now you can process the pickle files."

json:
	$(SPHINXBUILD) -b json $(ALLSPHINXOPTS) _build/json
	@echo
	@echo "Build finished; now you can process the JSON files."

htmlhelp:
	$(SPHINXBUILD) -b htmlhelp $(ALLSPHINXOPTS) _build/htmlhelp
	@echo
	@echo "Build finished; now you can run HTML Help Workshop with the" \
	      ".hhp project file in _build/htmlhelp."

qthelp:
	$(SPHINXBUILD) -b qthelp $(ALLSPHINXOPTS) _build/qthelp
	@echo
	@echo "Build finished; now you can run "qcollectiongenerator" with the" \
	      ".qhcp project file in _build/qthelp, like this:"
	@echo "# qcollectiongenerator _build/qthelp/pyflinkdoc.qhcp"
	@echo "To view the help file:"
	@echo "# assistant -collectionFile _build/qthelp/pyflinkdoc.qhc"

latex:
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) _build/latex
	@echo
	@echo "Build finished; the LaTeX files are in _build/latex."
	@echo "Run \`make all-pdf' or \`make all-ps' in that directory to" \
	      "run these through (pdf)latex."

changes:
	$(SPHINXBUILD) -b changes $(ALLSPHINXOPTS) _build/changes
	@echo
	@echo "The overview file is in _build/changes."

linkcheck:
	$(SPHINXBUILD) -b linkcheck $(ALLSPHINXOPTS) _build/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in _build/linkcheck/output.txt."

doctest:
	$(SPHINXBUILD) -b doctest $(ALLSPHINXOPTS) _build/doctest
	@echo "Testing of doctests in the sources finished, look at the " \
	      "results in _build/doctest/output.txt."

pdf: latex
	cd _build/latex && make all-pdf

pdf-html: pdf html
	cp _build/latex/pyflinkdoc.pdf _build/html
