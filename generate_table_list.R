# library
library(DBI)
library(odbc)
library(dplyr)
library(stringr)

# read in env file
readRenviron("C:/Users/BoydClaire/.Renviron")


# GET PRODUCTION TABLES
database = "production"
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "SQL Server",
                      Server   = Sys.getenv(paste0(database, "_server")),
                      Database = Sys.getenv(paste0(database, "_database")),
                      UID      = Sys.getenv(paste0(database, "_username")),
                      PWD      = Sys.getenv(paste0(database, "_password")),
                      TrustServerCertificate="yes",
                      Port     = 1433)

query <-
    glue::glue("
  SELECT *
  FROM
    information_schema.columns
    ;
")

production_columns <- dbGetQuery(con, query)

query = glue::glue("
    SELECT *
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
")

all_production_primary_keys <- dbGetQuery(con, query)

production_primary_keys = all_primary_keys %>%
    filter(!grepl("_MNC",COLUMN_NAME)) %>%
    group_by(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) %>%
    arrange(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION) %>%
    summarize(PRIMARY_KEYS = stringr::str_c(COLUMN_NAME, collapse = ", "), .groups = "drop")

production_tables = production_columns %>%
    group_by(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) %>%
    summarise(COLUMNS = n(),
              COLUMN_NAMES = stringr::str_c(COLUMN_NAME, collapse = ", "), .groups = "drop"
    ) %>%
    select(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMNS, COLUMN_NAMES) %>%
    mutate(first_col = str_extract(COLUMN_NAMES, "^[^,]+"),
           PREFIX = ifelse(grepl("_", first_col),
                           sub("^(.*?)_.*$", "\\1", first_col), NA)) %>%
    left_join(production_primary_keys, by=c("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"))

DBI::dbDisconnect(con)


# GET FDW AND IAS TABLES

database = "fdw"
database_schema = toupper(Sys.getenv(paste0(database, "_schema")))

# create database connection, by reading in the correct env variables
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "Oracle in OraClient19Home1",
                      DBQ   = Sys.getenv(paste0(database, "_path")),
                      DATABASE = Sys.getenv(paste0(database, "_schema")),
                      UID      = Sys.getenv(paste0(database, "_username")),
                      PWD      = Sys.getenv(paste0(database, "_password")),
                      TrustServerCertificate="no",
                      Port     = 1433)

query <-
    glue::glue("
    SELECT *
      FROM ALL_TAB_COLUMNS
      WHERE OWNER = '{toupper(Sys.getenv(paste0(database, '_schema')))}'

        ;
    ")

fdw_columns <- dbGetQuery(con, query)

fdw_tables = fdw_columns %>%
    mutate(TABLE_CATALOG = "FDW") %>%
    rename(TABLE_SCHEMA = OWNER) %>%
    group_by(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) %>%
    summarise(COLUMNS = n(),
              COLUMN_NAMES = stringr::str_c(COLUMN_NAME, collapse = ", "), .groups = "drop"
    ) %>%
    select(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMNS, COLUMN_NAMES) %>%
    mutate(first_col = str_extract(COLUMN_NAMES, "^[^,]+"),
           PREFIX = ifelse(grepl("_", first_col) & grepl("VW_CAMA", TABLE_NAME),
                           sub("^(.*?)_.*$", "\\1", first_col), NA))

DBI::dbDisconnect(con)


database = "ias"
database_schema = toupper(Sys.getenv(paste0(database, "_schema")))

# create database connection, by reading in the correct env variables
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "Oracle in OraClient19Home1",
                      DBQ   = Sys.getenv(paste0(database, "_path")),
                      DATABASE = Sys.getenv(paste0(database, "_schema")),
                      UID      = Sys.getenv(paste0(database, "_username")),
                      PWD      = Sys.getenv(paste0(database, "_password")),
                      TrustServerCertificate="no",
                      Port     = 1433)

query <-
    glue::glue("
    SELECT *
      FROM ALL_TAB_COLUMNS
      WHERE OWNER = '{toupper(Sys.getenv(paste0(database, '_schema')))}'

        ;
    ")

ias_columns <- dbGetQuery(con, query)

ias_tables = ias_columns %>%
    mutate(TABLE_CATALOG = "IAS") %>%
    rename(TABLE_SCHEMA = OWNER) %>%
    group_by(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) %>%
    summarise(COLUMNS = n(),
              COLUMN_NAMES = stringr::str_c(COLUMN_NAME, collapse = ", "), .groups = "drop"
    ) %>%
    select(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMNS, COLUMN_NAMES) %>%
    mutate(PREFIX = NA,
           first_col = NA)

DBI::dbDisconnect(con)


# BIND ALL TABLES TOGETHER

all_tables = rbind(production_tables, fdw_tables, ias_tables) %>%
    select(-first_col)


write.csv(all_tables, "all_tables.csv", na="")

# filter tables to only the ones we really use

table_list <- all_tables %>%
    rename(SCHEMA=TABLE_SCHEMA,
           COLS=COLUMNS,
           DATABASE=TABLE_CATALOG) %>%
    mutate(NOTES = NA) %>%
    filter(!stringr::str_ends(TABLE_NAME, '_20[0-9]{2}$') &
           !(SCHEMA %in% c("dbo", "COMMON", "internal", "DATA_BACKUP", "DataBackup", "CONV", "NYC_STAGE"))) %>%
    select(DATABASE, SCHEMA, TABLE_NAME, PREFIX, COLS, COLUMN_NAMES, NOTES)

write.csv(table_list, "table_list.csv", na="")


# match production // fdw tables
real_prop = production_tables %>%
    filter(TABLE_SCHEMA == "REAL_PROP")

fdw_crosswalk = fdw_tables %>%
    filter(grepl("VW_CAMA_", TABLE_NAME)) %>%
    mutate(CAMA_NAME = sub(".*VW_CAMA_", "", TABLE_NAME),
           HIST_CAMA_NAME = ifelse(grepl("_HIST", TABLE_NAME),
                                   sub("_HIST", "", sub(".*VW_CAMA_", "", TABLE_NAME)), NA)) %>%
    rename(FDW_NAME = TABLE_NAME) %>%
    select(FDW_NAME, CAMA_NAME, HIST_CAMA_NAME, COLUMNS) %>%
    # EXCEPTIONS FOR JOINING
    mutate(CAMA_NAME = ifelse(CAMA_NAME == "PARCEL_EXTENSIONS", "PARCEL_EXTENSION", CAMA_NAME)) %>%
    left_join(real_prop, by=c("CAMA_NAME"="TABLE_NAME")) # %>%
    #left_join(real_prop, by=c("HIST_CAMA_NAME"="TABLE_NAME"))


fdw_crosswalk = fdw_tables %>%
    filter(grepl("VW_CAMA_", TABLE_NAME) & grepl("_HIST", TABLE_NAME))







query <-
    glue::glue("
    SELECT *
      FROM FDW3NF.VW_CAMA_REALMAST
      WHERE ROWNUM <= 100
        ;
    ")

bldg <- dbGetQuery(con, query)

hist <-dbGetQuery(con, query)



