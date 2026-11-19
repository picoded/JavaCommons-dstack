# Changelog

All notable changes to `JavaCommons-dstack` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.1.18] - 2026-11-19

### Added
- Added `IndexDefinition` class representing normalized index specifications (name, keys with directions or custom types, unique, sparse, background, and TTL expiration).
- Added index management interface methods to `DataObjectMap`: `supportsIndexManagement()`, `listIndexes()`, `hasIndex(name)`, `hasIndexWithKeys(indexDef)`, and `createIndex(indexDef)`.
- Implemented MongoDB index management in `MongoDB_DataObjectMap`: `supportsIndexManagement()`, `listIndexes()` parsing existing indexes into `IndexDefinition`, and `createIndex(indexDef)` with pre-checks against duplicate names/keys and background building.
- Added index management forwarding in `Stack_DataObjectMap`.

### Added
- Added `queryIn` and `queryIn_id` to `DataObjectMap` for querying records matching any value in a collection (up to 100 items), with overloads supporting `additionalWhereQuery` and `additionalWhereArgs`.
- Implemented backend-specific `queryIn_id` support in `JSql_DataObjectMap`, `PostgresJsonb_DataObjectMap`, `MongoDB_DataObjectMap`, and `Stack_DataObjectMap`.

## [4.1.16] - 2026-11-16

### Added
- Added `removeBulk(Collection<String> oids)` and `removeBulk(String[] oids)` to `DataObjectMap` for bulk deletion across storage backends with a 500-item maximum limit.
- Implemented native bulk deletion support in `JSql_DataObjectMap`, `MongoDB_DataObjectMap`, `PostgresJsonb_DataObjectMap`, and `Stack_DataObjectMap`.
