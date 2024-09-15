-- Databricks notebook source
-- CREATE SOURCE TABLE
CREATE TABLE IF NOT EXISTS source_table (
    id INT,
    name STRING,
    updated_at TIMESTAMP
);

-- Insert some initial data into source_table
INSERT INTO source_table VALUES
(1, 'Alice', '2023-08-01 12:00:00'),
(2, 'Bob', '2023-08-01 12:00:00');

SELECT * FROM source_table

-- COMMAND ----------

-- CREATE DESTINATION TABLE
CREATE TABLE IF NOT EXISTS target_table (
    id INT,
    name STRING,
    updated_at TIMESTAMP
);

-- Initially, let's load data from source_table into target_table
INSERT INTO target_table
SELECT * FROM source_table;



-- COMMAND ----------

select * from target_table

-- COMMAND ----------

-- Insert new and updated records into source_table
INSERT INTO source_table VALUES
(2, 'Bob Updated', '2023-08-02 12:00:00'), -- Updated record
(3, 'Charlie', '2023-08-02 12:00:00');    -- New record

-- COMMAND ----------

select * from source_table

-- COMMAND ----------

-- Merge data into destination table
MERGE INTO target_table AS target
USING (
    SELECT id, name, updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM source_table
    ) tmp
    WHERE rn = 1
) AS source
ON target.id = source.id
WHEN MATCHED AND target.updated_at < source.updated_at THEN
    UPDATE SET target.name = source.name, target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (id, name, updated_at) VALUES (source.id, source.name, source.updated_at);

-- COMMAND ----------

select * from target_table
