SET NOCOUNT ON;
IF EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE object_id = OBJECT_ID('{TARGET_TABLE}')
                        AND index_id > 0
                        )
                    AND EXISTS (
                        SELECT 1
                        FROM {TARGET_TABLE}
                    )
                    BEGIN
                        SELECT
                            1,
                            'Initial load blocked. {TARGET_TABLE} contains rows and has indexes.'
                                AS PreCheckMessage;
                    END
                ELSE IF EXISTS (
                                SELECT 1
                                FROM sys.indexes
                                WHERE object_id = OBJECT_ID('{TARGET_TABLE}')
                                AND index_id > 0
                            )
                            BEGIN
                                SELECT
                                    1,
                                    'Initial load blocked. Indexes exist on {TARGET_TABLE}.'
                                        AS PreCheckMessage;
                            END
                    ELSE IF EXISTS (
                            SELECT 1
                            FROM {TARGET_TABLE}
                        )
                        BEGIN
                            SELECT
                                1,
                                'Initial load blocked. {TARGET_TABLE} contains rows.'
                                    AS PreCheckMessage;
                        END
                    ELSE
                        BEGIN
                            SELECT
                                0,
                                'Initial load permitted. {TARGET_TABLE} contains no rows and has no indexes.'
                                    AS PreCheckMessage;
                        END;
