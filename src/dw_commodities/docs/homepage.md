{% docs __overview__ %}

# DBT-Core Project for a Commodities Data Warehouse

This project uses DBT (Data Build Tool) to manage and transform data in a Commodities Data Warehouse (DW).
Its goal is to create a robust and efficient data pipeline to process and organize commodities data and their movements for analysis.

## Project Structure

### 1. Seeds

Seeds are static datasets loaded into the Data Warehouse from CSV files. In this project, seeds are used to load data about commodities movements.

### 2. Models

Models define data transformations using SQL. They are split into two main layers: staging and datamart.

#### Staging

The staging layer prepares and cleans the data before it is loaded into the final analytical tables.

- **stg_commodities.sql**: Cleans and formats the commodities data extracted from the API.
- **stg_movimentacao_commodities.sql**: Cleans and formats the commodities movement data.

#### Datamart

The datamart layer contains the final analytical tables, based on the data prepared in the staging layer.

- **dm_commodities.sql**: Integrates the cleaned commodities and movement data, creating the final model for analysis.

### 3. Sources

Sources are the original tables or files that DBT uses for transformations.


## Directory Structure

```plaintext
├── models
│   ├── staging
│   │   ├── stg_commodities.sql
│   │   └── stg_movimentacao_commodities.sql
│   └── datamart
│       └── dm_commodities.sql
├── seeds
│   └── movimentacao_commodities.csv
├── dbt_project.yml
└── README.md
```

### Running the Project

1. **Clone o Repository**:
   ```bash
   git clone git@github.com:marcelopachione/end2end-dw-commodities.git
   cd end2end-dw-commodities/src/dw_commodities
   ```

2. **Install DBT**:
   ```bash
   pip install dbt-core dbt-postgres
   ```

3. **Config DBT**:
   - Edit the profiles.yml file with the connection details for your Data Warehouse:

   Example de `profiles.yml`:
   ```yaml
   databasesales:
     target: dev
     outputs:
       dev:
         type: postgres
         host: <DB_HOST_PROD>
         user: <DB_USER_PROD>
         password: <DB_PASS_PROD>
         port: <DB_PORT_PROD>
         dbname: <DB_NAME_PROD>
         schema: <DB_SCHEMA_PROD>
         threads: 1
   ```

4. **Execute Seeds DBT**:
   ```bash
   dbt seed
   ```

5. **Execute Transform DBT**:
   ```bash
   dbt run
   ```

---

### Models Description

#### stg_commodities.sql

This model cleans and formats the commodities data pulled from the API. It takes care of all the necessary cleanup and transformations to get the data ready for the datamart.

#### stg_movimentacao_commodities.sql

This model cleans and formats the commodities movement data. It applies the needed cleanup and transformations so the data is prepared for the datamart.

#### dm_commodities.sql

This model brings together the cleaned commodities and movement data to create the final data model for analysis. It also calculates key metrics and aggregates the data to make it easier to explore and visualize in the dashboard.

{% enddocs %}