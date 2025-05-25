# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).


## [0.12.0] - 26-05-2025
 
### Added
- New `rules_version` key to the expected data quality rules JSON format, for rules versioning at the table level

### Changed
 
### Fixed
- Fixed broken/inactive `TestGetParametersFromResults` in `test_output_transformations.py`


## [0.11.19] - 15-05-2025
 
### Added

### Changed
- Refactor `output_transformations.py`
- Modify `Rule` and `ValidationSettings` dataclasses
 
### Fixed
 
## [0.11.18] - 14-05-2025

### Added

### Changed

### Fixed
- Fix for Slack messages resulting from `ExpectTableColumnsToMatchSet` expectations

## [0.11.17] - 30-04-2025

### Added

### Changed

### Fixed
- Allow for Slack messages resulting from validations of empty source files

## [0.11.16] - 30-04-2025
 
### Added
- Changes are added to the CHANGELOG.md file
### Changed
 
### Fixed

## [0.11.15] - 19-03-2025
 
### Added

### Changed
 
### Fixed
- Formatting for Slack messages

## [0.11.0] - 11-2024
 
### Added

### Changed
- Stability and testability improvements
### Fixed

## [0.10.0] - 10-2024
 
### Added

### Changed
- Switched to GX 1.0
### Fixed

## [0.9.0] - 9-2024
 
### Added
- Added dataset descriptions
### Changed

### Fixed

## [0.8.0] - 8-2024
 
### Added
- Implemented output historization
### Changed

### Fixed

## [0.7.0] - 7-2024
 
### Added

### Changed

### Fixed
- Refactored the solution

## [0.6.0] - 6-2024
 
### Added

### Changed
- The results are written to tables in the "dataquality" schema
### Fixed

## [0.5.0] - 5-2024
 
### Added
- Export schema from Unity Catalog
### Changed

### Fixed

## [0.4.0] - 4-2024
 
### Added
- Added schema validation with Amsterdam Schema per table
### Changed

### Fixed

## [0.3.0] - 3-2024
 
### Added

### Changed

### Fixed
- Refactored I/O

## [0.2.0] - 2-2024
 
### Added
- Run a DQ check for multiple dataframes
### Changed

### Fixed

## [0.1.0] - 1-2024
 
### Added
- Run a DQ check for a dataframe
### Changed

### Fixed
