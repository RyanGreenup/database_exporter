# Limitations

## Windows Support

Duckdb is not supported on Windows [^1738559984], as a result the feature is locked behind the `"duckdb"` feature flag.

[^1738559984]: I'm not sure why, PR's welcome, GDB returns the following (seems like a `.dll` issue):
    ```sh
    gdb .\extract_to_sqlite_rs.exe
    run
    bt
    ```
    ```
    During startup program exited with code `0xc0000135`
    ```

    I have tried:

    ```sh
    winget install DuckDB.cli
    scoop install duckdb
    ```

    However, it did not help

## SQLite Types

I've had some issues with the `NUMERIC` type in SQLite, this causes Rusqlite to pass an error up to Connector-X like so:

```rust
called `Result::unwrap()` on an `Err` value:

  ArrowError(
      SQLiteArrowTransportError(
          Source(
              SQLiteError(
                  InvalidColumnType( 7, "latitude", Real)))))


```

To overcome this create a new table with the same data:

```sql
-- Step 1: Create a new table with the desired REAL type
CREATE TABLE new_notes_test (
    id TEXT PRIMARY KEY,
    latitude NUMERIC
);

-- Step 2: Copy data from the old table to the new table
INSERT INTO new_notes_test (id, value)
SELECT id, latitude
FROM notes;

```

From here one may:

1. Keep the New Table

    ```sql
    -- Step 3: Drop the old table
    DROP TABLE notes;

    -- Step 4: Rename the new table to the original table name
    ALTER TABLE new_notes_test
    RENAME TO notes;
    ```

2. Keep the Old Table

    ```
    DROP TABLE new_notes_test
    ```

3. Keep Both Tables

    Keep second table with a different type to preserve all previous behaviour but allow exporting with this tool.

    This requires a few triggers:

    1. Insert Trigger

        ```sql
        CREATE TRIGGER insert_trigger
        AFTER INSERT ON ExampleTable
        BEGIN
            INSERT INTO NewExampleTable (id, value)
            VALUES (NEW.id, NEW.value);
        END;
        ```

    2. Update Trigger
        ```sql
        CREATE TRIGGER update_trigger
        AFTER UPDATE ON ExampleTable
        BEGIN
            UPDATE NewExampleTable
            SET value = NEW.value
            WHERE id = OLD.id;
        END;
        ```

    3. Read Trigger

        ```sql
        CREATE TRIGGER delete_trigger
        AFTER DELETE ON ExampleTable
        BEGIN
            DELETE FROM NewExampleTable
            WHERE id = OLD.id;
        END;
        ```

