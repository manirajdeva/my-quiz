CREATE OR REPLACE TABLE ALL_OBJECT_DDLS (
    OBJECT_TYPE   STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME   STRING,
    OBJECT_NAME   STRING,
    DDL_TEXT      STRING
);

CREATE OR REPLACE PROCEDURE LOAD_TABLE_DDLS(DB_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var inserted = 0;

// Clear previous entries
var clear_stmt = snowflake.createStatement({
    sqlText: `TRUNCATE TABLE ALL_OBJECT_DDLS`
});
clear_stmt.execute();

// Get all tables in the database
var show_sql = `SHOW TABLES IN DATABASE ` + DB_NAME;
var show_stmt = snowflake.createStatement({sqlText: show_sql});
var result = show_stmt.execute();

// Loop through tables
while (result.next()) {
    var schema_name = result.getColumnValue("schema_name");
    var table_name  = result.getColumnValue("name");
    var fq_name     = DB_NAME + "." + schema_name + "." + table_name;

    // Get the DDL
    var ddl_stmt = snowflake.createStatement({
        sqlText: `SELECT GET_DDL('TABLE', '${fq_name}')`
    });
    var ddl_result = ddl_stmt.execute();
    ddl_result.next();
    var ddl_text = ddl_result.getColumnValue(1);

    // Insert into the DDL table
    var ins_stmt = snowflake.createStatement({
        sqlText: `INSERT INTO ALL_OBJECT_DDLS 
                  (OBJECT_TYPE, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, DDL_TEXT)
                  VALUES (?, ?, ?, ?, ?)`,
        binds: ['TABLE', DB_NAME, schema_name, table_name, ddl_text]
    });
    ins_stmt.execute();

    inserted++;
}

return 'Inserted ' + inserted + ' table DDLs into ALL_OBJECT_DDLS';
$$;
