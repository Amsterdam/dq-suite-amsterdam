# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.12.6] - 15-10-2025
 
### Added
- Added Table-level expectation result fields to be populated in the Afwijking table

### Changed
- Extended `get_single_expectation_afwijking_data` function logic

### Fixed
- Refactored I/O

## [0.12.03] - 08-09-2025
 
### Added

### Changed
- Normalize parameters for consistent regelId, and process `observed_value` for `ExpectTableRowCountToEqual` and `ExpectTableRowCountToBeBetween` rules.
=======
## [0.12.00] - 05-09-2025
 
### Added
- Added  `profile_and_create_rules` function and placed in `profile` folder

## [0.11.21] - 12-08-2025
 
### Added

### Changed
- Rule names reformatted in output to match with input rule name.
  
## [0.11.20] - 18-07-2025
 
### Added

### Changed
- Added `highest severity level` to output
- Modify `Rule` dataclass to include a severity field

## [0.11.19] - 15-05-2025
 
### Added

### Changeds
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
