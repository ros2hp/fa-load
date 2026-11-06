SET NOCOUNT ON;
DECLARE @DuplicateCheck BIT = 1;
                DECLARE @BeginBalCheck    BIT = 1;
                DECLARE @CreditCheck     BIT = 1;

                -- Duplicate Check
                SELECT @DuplicateCheck =
                    CASE
                        WHEN EXISTS
                        (
                            SELECT 1
                            FROM {TARGET_TABLE}
                            WHERE NOT (
                                account LIKE 'Total for FUND %'
                                OR account = ''
                                OR account = 'End of Report'
                                OR account IS NULL
                            )
                            GROUP BY fund, year, month, account_long
                            HAVING COUNT(*) > 1
                        )
                        THEN 1
                        ELSE 0
                    END;

                -- Begin Balance Check Validation
                IF EXISTS
                (
                    SELECT 1
                    FROM {SCHEMA}.gl_tb_metadata m
                    INNER JOIN
                    (
                        SELECT
                            x_run_id,
                            fund,
                            month,
                            year,
                            SUM(
                                TRY_CAST(REPLACE(begining_balance_AUD, ',', '') AS DECIMAL(18,2))
                            ) AS begining_balance
                        FROM {TARGET_TABLE}
                        WHERE LEN(account) = 6
                        GROUP BY
                            x_run_id,
                            fund,
                            month,
                            year
                    ) beg_bal
                        ON m.x_run_id = beg_bal.x_run_id
                    AND m.x_fund   = beg_bal.fund
                    AND m.x_month  = beg_bal.month
                    AND m.x_year   = beg_bal.year
                    WHERE TRY_CAST(REPLACE(m.beginning_balance_aud_total, ',', '') AS DECIMAL(18,2))
                        <> beg_bal.begining_balance
                )
                    SET @BeginBalCheck = 1;
                ELSE
                    SET @BeginBalCheck = 0;

                -- Credit Validation
                IF EXISTS
                (
                    SELECT 1
                    FROM {SCHEMA}.gl_tb_metadata m
                    INNER JOIN
                    (
                        SELECT
                            x_run_id,
                            fund,
                            month,
                            year,
                            SUM(
                                TRY_CAST(REPLACE(credits_AUD, ',', '') AS DECIMAL(18,2))
                            ) AS credit_aud_tot
                        FROM {TARGET_TABLE}
                        WHERE LEN(account) = 6
                        GROUP BY
                            x_run_id,
                            fund,
                            month,
                            year
                    ) credits
                        ON m.x_run_id = credits.x_run_id
                    AND m.x_fund   = credits.fund
                    AND m.x_month  = credits.month
                    AND m.x_year   = credits.year
                    WHERE TRY_CAST(REPLACE(m.credits_aud_total, ',', '') AS DECIMAL(18,2))
                        <> credits.credit_aud_tot
                )
                    SET @CreditCheck = 1;
                ELSE
                    SET @CreditCheck = 0;

                -- Final Result
                SELECT
                    CASE
                        WHEN @DuplicateCheck = 1
                          OR @CreditCheck = 1
                          OR @BeginBalCheck = 1
                        THEN 1
                        ELSE 0
                    END AS ValidationResult,

                    CASE
                        WHEN @DuplicateCheck = 1
                        THEN 'Has duplicates'

                        WHEN @CreditCheck = 1
                          OR @BeginBalCheck = 1
                        THEN 'Failed credit or begin balance validation'

                        ELSE 'Has no duplicates and credits & begin balances validated (implies no missing data)'
                    END AS PostCheckMessage;
