# Makefile for Sphinx documentation

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = vector_to_parquet_processor
SOURCEDIR     = source
BUILDDIR      = build

.PHONY: help clean html latexpdf

help:
	@echo "Please use \`make <target>\` where <target> is one of"
	@echo "  html      to make standalone HTML files"
	@echo "  latexpdf  to make LaTeX files and run them through pdflatex"
	@echo "  clean     to clean the built documentation"

# Catch-all target: route all unknown targets to Sphinx using the builder type.
%:
	$(SPHINXBUILD) -b $@ $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/$@ || exit $?
	@echo "Build finished. The $@ pages are in $(BUILDDIR)/$@."

# Shortcut for "make html"
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/html || exit $?
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

# Shortcut for "make latexpdf"
latexpdf:
	$(SPHINXBUILD) -b latex $(SPHINXOPTS) $(SOURCEDIR) $(BUILDDIR)/latex || exit $?
	@echo "Building LaTeX files..."
	@cd $(BUILDDIR)/latex && make all-pdf || exit $?
	@echo "Build finished. The PDF file is in $(BUILDDIR)/latex."

# Clean up build directories
clean:
	-rm -rf $(BUILDDIR)/*
	@echo "Cleaned build directories."
