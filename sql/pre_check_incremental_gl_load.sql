SET NOCOUNT ON;
IF NOT EXISTS
                    (
                        SELECT 1
                        FROM {SCHEMA}.run$log
                        WHERE operation = 'Initial_GL_Load'
                        AND environment = '{ENVIRONMENT}'
                        AND status = 'SUCCESS'
                    )
                    BEGIN
                        Select
                            1,
                            'Incremental load blocked. No completed Initial_GL_Load found for {ENVIRONMENT} environment.';
                    END;
                ELSE
                    IF NOT EXISTS (
                            SELECT 1
                            FROM {TARGET_TABLE}
                        )
                        BEGIN
                            SELECT
                                1,
                                'Incremental load blocked. {TARGET_TABLE} contains no rows.'
                                    AS PreCheckMessage;
                        END
                    ELSE
                        BEGIN
                            SELECT
                                0,
                                'Incremental load permitted. {TARGET_TABLE} contains rows'
                                    AS PreCheckMessage;
                        END;
