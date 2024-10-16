# ETL Testing Tool

## Overview
**ETL Testing Tool**: A Streamlit-based application developed using Python to validate various aspects of ETL processes. This tool is designed to ensure data integrity during ETL operations by comparing source and destination data.

## Features
- **Data Validation**: Validate row counts between source and destination tables.
- **Column Validation**: Ensure all expected columns are present in the destination.
- **Data Type Validation**: Check data types of columns between source and destination.
- **Constraints Validation**: Verify that null constraints and other data integrity rules are correctly applied during migration.

## Technology Stack
- **Python**: Core programming language for implementing the validation logic.
- **Streamlit**: Front-end framework to create an interactive user interface.
- **Pandas**: Data analysis and manipulation library used for data comparison.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/etl-testing-tool.git
   cd etl-testing-tool
