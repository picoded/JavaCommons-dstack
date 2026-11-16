# Changelog

All notable changes to `JavaCommons-dstack` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.1.16] - 2026-11-16

### Added
- Added `removeBulk(Collection<String> oids)` and `removeBulk(String[] oids)` to `DataObjectMap` for bulk deletion across storage backends with a 500-item maximum limit.
- Implemented native bulk deletion support in `JSql_DataObjectMap`, `MongoDB_DataObjectMap`, `PostgresJsonb_DataObjectMap`, and `Stack_DataObjectMap`.
