# Job Market Analytics

The aim of this project is to develop an end-to-end Data Platform to explore and learn new technologies.

## Architecture

![Architecture Overview](doc/architecture-overview.drawio.svg)

## Storage

### Data Lake

The Data Lake is basically a file system on my local computer, but could be easily transfered to a Cloud Blob Storage (
like AWS S3 or Azure Blob Storage) if needed. The current Data Lake we have two layers:

- The Raw Layer, where the information from the data source are stored in the same file format as ingested (e.g. HTML or
  XML).
- The Cleansed Layer, where we store the information in Parquet, which means that the information is stored in a tabular
  format with well-defined columns.

### Data Warehouse

The Data Warehouse is based on PostgreSQL plus an extension in order to read Parquet files as foreign tables. PostgreSQL
might not be the best choice for a datawarehouse since it is row-column-oriented but in this case we have reduced number
of columns and a relative small data size. Another advantage of PostgreSQL is that I can run it easily on my computer
via Docker so that I can avoid cloud service costs. We will divide the datawarehouse in 3 schemas:

- Staging, which are basically foreign tables referencing the Parquet files on the Data Lake Cleansed Layer.
- Data Vault, where the data is modelled and historized using
  the [Data Vault Specification](https://danlinstedt.com/wp-content/uploads/2018/06/DVModelingSpecs2-0-1.pdf).
- Data Mart, which will be the consuming layer for our BI Tool.