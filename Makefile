# Makefile for the Kognitos Battering Ram Demo
# This is the single entry point for the user.

# Use bash for all commands
SHELL := /bin/bash

# Define python executable from poetry's virtualenv
PYTHON := $(shell poetry run which python)

.PHONY: setup clean demo test

# The main command to run the entire demo
demo: clean setup
	@$(PYTHON) benchmark.py

# Sets up the environment: installs dependencies, creates DB, generates data
setup:
	@echo "🛠️  Setting up environment..."
	@poetry install --no-root
	@echo "📄 Generating synthetic invoice data..."
	@$(PYTHON) generate_invoices.py

# Cleans up generated files for a fresh run
clean:
	@echo "🧹 Cleaning up previous run..."
	@rm -f results.db
	@rm -rf data

# Placeholder for running tests
test:
	@echo "🧪 Running tests..."
	@poetry run pytest
