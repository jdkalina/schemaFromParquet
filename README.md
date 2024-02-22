# SchemaFromParquet

This Python script facilitates the conversion of Parquet files to a JSON representation of a schema suitable for use in Kx Systems' kdb+ database.

## Usage

### Installation

Ensure you have the necessary dependencies installed:

```bash
pip install pyarrow
```

### Description

The script consists of several functions:

- `cvEntry(fname, ftype)`: This function is used to create a dictionary entry representing a column in the schema. It maps Parquet types to corresponding Kx types.
- `schema_from_parquet(filename)`: This function reads the schema of a Parquet file using the PyArrow library and converts it into a list of dictionaries representing the columns in the schema.
- `table_creator(tableName, columns, timeCol)`: This function creates a JSON object representing a table in the schema, including its columns, type, and time column.
- `multiple_time_cols(timeCols)`: This function prompts the user to choose from multiple time columns.
- `pick_time(tablename, columns)`: This function selects the time column for partitioning the table.
- `fromParquet(directory)`: This function processes Parquet files in a given directory, extracting schema information for each file and generating a JSON representation of the schema.

## Example

```python
from kxInsights.database.config.parquetSchemaFormatter import *  
directory = '/path/to/parquet/files' 
schema = fromParquet(directory) 
print(schema)
```

Replace `/path/to/parquet/files` with the path to the directory containing your Parquet files. This will generate a JSON representation of the schema for each Parquet file in the specified directory.
