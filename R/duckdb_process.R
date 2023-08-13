# Load necessary libraries
library(DBI)
library(dplyr)
library(duckdb)
library(haven) # to read .dta files
library(arrow) # to write parquet files

# Define a class to handle database operations
db_handler <- R6::R6Class(
  "db_handler",
  
  public = list(
    connection = NULL,
    
    initialize = function(dbdir = ":memory:") {
      self$connection <- dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = FALSE)
    },
    
    create_table = function(query) {
      dbExecute(self$connection, query)
    },
    
    write_to_db = function(dataframe, table_name) {
      dbWriteTable(self$connection, table_name, dataframe)
    },
    
    query_db = function(query, params = NULL) {
      dbGetQuery(self$connection, query, params)
    },
    
    close_connection = function() {
      dbDisconnect(self$connection)
    }
  )
)

# Define a class to handle file operations
file_handler <- R6::R6Class(
  "file_handler",
  
  public = list(
    convert_and_load_files = function(dta_files, db_handler) {
      for (file in dta_files) {
        # Read .dta file
        dta_data <- read_dta(file)
        
        # Convert to parquet
        parquet_file <- gsub(".dta", ".parquet", file)
        arrow::write_parquet(dta_data, parquet_file)
        
        # Register parquet with DuckDB
        tbl_name <- tools::file_path_sans_ext(basename(file))
        duckdb::duckdb_register(db_handler$connection, tbl_name, arrow::read_parquet(parquet_file))
      }
    }
  )
)

# Example usage
db <- db_handler$new(dbdir = "my-db.duckdb")
file_op <- file_handler$new()
file_op$convert_and_load_files(c("data/rankings.dta"), db)

# Query and print as needed
result <- db$query_db("SELECT * FROM rankings LIMIT 100")
print(result)

# Clean up
db$close_connection()
