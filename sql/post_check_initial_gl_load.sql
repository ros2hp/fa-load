SET NOCOUNT ON;
DECLARE @has_duplicates int = 0;
            DECLARE @has_nulls int = 0;
            IF NOT EXISTS
                (
                    SELECT 1
                    FROM sys.indexes
                    WHERE name = 'dup_idx'
                    AND object_id = OBJECT_ID('{TARGET_TABLE}')
                )
                BEGIN
                    CREATE NONCLUSTERED INDEX dup_idx
                    ON {TARGET_TABLE}
                    (
                        JE_HEADER_ID,
                        JE_LINE_NUM
                    )
                    WITH
                    (
                        MAXDOP = 8,
                        SORT_IN_TEMPDB = ON
                    );
                END;

            UPDATE STATISTICS {TARGET_TABLE} WITH FULLSCAN;

            -- Check for duplicates
            SELECT @has_duplicates =
                CASE
                    WHEN EXISTS
                    (
                        SELECT 1
                        FROM {TARGET_TABLE}
                        GROUP BY
                            JE_HEADER_ID,
                            JE_LINE_NUM
                        HAVING COUNT(*) > 1
                    )
                    THEN 1
                    ELSE 0
                END;

            -- Check for NULL values
            SELECT @has_nulls =
                CASE
                    WHEN EXISTS
                    (
                        SELECT 1
                        FROM {TARGET_TABLE}
                        WHERE JE_HEADER_ID IS NULL
                        OR JE_LINE_NUM IS NULL
                    )
                    THEN 1
                    ELSE 0
                END;

            IF @has_duplicates > 0 OR @has_nulls > 0
            BEGIN
                SELECT
                    1,
                    CONCAT(
                        CASE WHEN @has_duplicates > 0 THEN 'Duplicate JE_HEADER_ID/JE_LINE_NUM found. ' ELSE '' END,
                        CASE WHEN @has_nulls > 0 THEN 'NULL values found in JE_HEADER_ID or JE_LINE_NUM.' ELSE '' END
                    ) AS PostCheckMessage;
            END
            ELSE
            BEGIN
                SELECT
                    0,
                    'No duplicates or NULL values found.'
                        AS PostCheckMessage;
            END;
