
## Understanding Fact Data Modeling

Fact data modeling focuses on capturing measurable events and metrics, often involving time-series data that needs to be aggregated and analyzed efficiently. The key concepts we'll cover include:

- **Deduplication**: Removing duplicate records based on business logic
- **Cumulative Tables**: Building historical snapshots that accumulate data over time
- **Datelist Patterns**: Efficient storage of user activity using arrays and bit manipulation
- **Reduced Fact Tables**: Pre-aggregated tables for faster analytics

## Query 1: Deduplication of game_details

```sql
-- Remove duplicates from game_details table
WITH deduplicated_games AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY game_id, team_id, player_id 
               ORDER BY game_id
           ) as row_num
    FROM game_details
)
SELECT 
    game_id,
    team_id, 
    player_id,
    player_name,
    start_position,
    comment
FROM deduplicated_games 
WHERE row_num = 1;
```

**Explanation**: This query uses the `ROW_NUMBER()` window function to identify duplicates[1][2]. We partition by the combination of `game_id`, `team_id`, and `player_id` since these three fields together should uniquely identify a record. The `ORDER BY game_id` ensures consistent ordering. We keep only the first occurrence (`row_num = 1`) of each duplicate group[2][3].

**Learning Point**: Deduplication is crucial in data engineering because duplicate data can skew analytics and waste storage. Always identify the business key (combination of columns that should be unique) before deduplicating.

## Query 2: DDL for user_devices_cumulated Table

```sql
CREATE TABLE user_devices_cumulated (
    user_id BIGINT,
    device_activity_datelist MAP>,
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- Alternative approach with separate rows per browser type:
CREATE TABLE user_devices_cumulated_alt (
    user_id BIGINT,
    browser_type STRING,
    activity_dates ARRAY,
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);
```

**Explanation**: This table uses a **cumulative design pattern** where each row represents a user's complete activity history up to a specific date[4][5]. The `MAP>` structure allows us to track different browser types and their associated active dates for each user. The date column represents the "as of" date for this snapshot.

**Learning Point**: Cumulative tables trade storage space for query performance. Instead of scanning millions of daily events, you can query a single row per user to get their complete history.

## Query 3: Cumulative Query for device_activity_datelist

```sql
WITH yesterday AS (
    SELECT * 
    FROM user_devices_cumulated 
    WHERE date = DATE('2023-01-31')
),
today AS (
    SELECT 
        user_id,
        browser_type,
        DATE('2023-02-01') as activity_date,
        COUNT(1) as num_events
    FROM events 
    WHERE DATE(event_time) = DATE('2023-02-01')
        AND user_id IS NOT NULL
    GROUP BY user_id, browser_type
)

INSERT INTO user_devices_cumulated
SELECT 
    COALESCE(t.user_id, y.user_id) as user_id,
    CASE 
        WHEN y.device_activity_datelist IS NULL THEN 
            MAP(ARRAY[t.browser_type], ARRAY[ARRAY[t.activity_date]])
        WHEN y.device_activity_datelist[t.browser_type] IS NULL THEN
            MAP_CONCAT(y.device_activity_datelist, 
                      MAP(ARRAY[t.browser_type], ARRAY[ARRAY[t.activity_date]]))
        ELSE
            MAP_CONCAT(y.device_activity_datelist,
                      MAP(ARRAY[t.browser_type], 
                          ARRAY[ARRAY_CONCAT(y.device_activity_datelist[t.browser_type], 
                                           ARRAY[t.activity_date])]))
    END as device_activity_datelist,
    DATE('2023-02-01') as date
FROM yesterday y
FULL OUTER JOIN today t ON y.user_id = t.user_id;
```

**Explanation**: This follows the **incremental cumulative pattern**[6][4]. We take yesterday's complete state and today's new activity, then merge them using a `FULL OUTER JOIN`. The `COALESCE` handles new users (not in yesterday) and inactive users (not in today). The complex `CASE` statement manages the MAP structure, either creating new entries or appending to existing arrays.

**Learning Point**: The FULL OUTER JOIN pattern is fundamental in cumulative table design. It ensures we don't lose historical data (yesterday) or miss new activity (today).

## Query 4: Datelist Int Generation

```sql
WITH date_series AS (
    SELECT generate_series(
        DATE('2023-01-01'), 
        DATE('2023-01-31'), 
        INTERVAL '1 day'
    ) as series_date
),
user_device_expanded AS (
    SELECT 
        user_id,
        browser_type,
        activity_date,
        DATE('2023-01-31') as current_date
    FROM user_devices_cumulated
    CROSS JOIN UNNEST(device_activity_datelist[browser_type]) as t(activity_date)
    WHERE date = DATE('2023-01-31')
),
datelist_calculation AS (
    SELECT 
        ude.user_id,
        ude.browser_type,
        ds.series_date,
        CASE WHEN ude.activity_date = ds.series_date THEN 1 ELSE 0 END as is_active,
        31 - EXTRACT(DAY FROM ds.series_date) as days_since
    FROM date_series ds
    CROSS JOIN (SELECT DISTINCT user_id, browser_type FROM user_device_expanded) users
    LEFT JOIN user_device_expanded ude 
        ON users.user_id = ude.user_id 
        AND users.browser_type = ude.browser_type
        AND ude.activity_date = ds.series_date
)

SELECT 
    user_id,
    browser_type,
    SUM(CASE WHEN is_active = 1 THEN POW(2, days_since) ELSE 0 END)::BIGINT as datelist_int
FROM datelist_calculation
GROUP BY user_id, browser_type;
```

**Explanation**: This converts date arrays into compact integer representations using **bit manipulation**[1][7][5]. Each bit position represents a specific date, with the rightmost bit being the most recent date. If a user was active on a date, that bit is set to 1. This technique can compress 31 days of activity into a single 32-bit integer, dramatically reducing storage requirements.

**Learning Point**: Datelist integers are extremely efficient for calculating metrics like "active in last 7 days" using bitwise operations, but they're limited by integer size (typically 32 or 64 bits).

## Query 5: DDL for hosts_cumulated Table

```sql
CREATE TABLE hosts_cumulated (
    host STRING,
    host_activity_datelist ARRAY,
    date DATE,
    PRIMARY KEY (host, date)
);
```

**Explanation**: This follows the same cumulative pattern but tracks host activity instead of user activity. Each row contains a host's complete activity history up to the specified date.

## Query 6: Incremental Query for host_activity_datelist

```sql
WITH yesterday AS (
    SELECT * 
    FROM hosts_cumulated 
    WHERE date = DATE('2023-01-31')
),
today AS (
    SELECT 
        host,
        DATE('2023-02-01') as activity_date,
        COUNT(1) as num_events
    FROM events 
    WHERE DATE(event_time) = DATE('2023-02-01')
        AND host IS NOT NULL
    GROUP BY host
)

INSERT INTO hosts_cumulated
SELECT 
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.activity_date]
        WHEN t.host IS NOT NULL THEN 
            ARRAY_CONCAT(y.host_activity_datelist, ARRAY[t.activity_date])
        ELSE y.host_activity_datelist
    END as host_activity_datelist,
    DATE('2023-02-01') as date
FROM yesterday y
FULL OUTER JOIN today t ON y.host = t.host;
```

**Explanation**: Similar to the user devices query, this incrementally builds host activity history[6]. The logic is simpler since we're dealing with a single array rather than a map structure.

## Query 7: DDL for host_activity_reduced Table

```sql
CREATE TABLE host_activity_reduced (
    month DATE,
    host STRING,
    hit_array ARRAY,
    unique_visitors_array ARRAY,
    PRIMARY KEY (month, host)
);
```

**Explanation**: This is a **reduced fact table** that pre-aggregates daily metrics into monthly arrays[8][9]. Each array position represents a day of the month, storing the count of hits and unique visitors. This design enables fast monthly reporting while preserving daily granularity.

**Learning Point**: Reduced fact tables are a common pattern for balancing storage efficiency with query performance. They're particularly useful for dashboards and reports that need fast aggregation.

## Query 8: Incremental Query for host_activity_reduced

```sql
WITH yesterday AS (
    SELECT * 
    FROM host_activity_reduced 
    WHERE month = DATE_TRUNC('month', DATE('2023-02-01'))
),
today AS (
    SELECT 
        host,
        DATE('2023-02-01') as event_date,
        COUNT(1) as hit_count,
        COUNT(DISTINCT user_id) as unique_visitors
    FROM events 
    WHERE DATE(event_time) = DATE('2023-02-01')
        AND host IS NOT NULL
    GROUP BY host
),
daily_position AS (
    SELECT 
        *,
        EXTRACT(DAY FROM event_date) as day_of_month
    FROM today
)

INSERT INTO host_activity_reduced
SELECT 
    DATE_TRUNC('month', DATE('2023-02-01')) as month,
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.hit_array IS NULL THEN 
            ARRAY_FILL(0::BIGINT, ARRAY[t.day_of_month - 1]) || ARRAY[t.hit_count]
        ELSE 
            y.hit_array || ARRAY[COALESCE(t.hit_count, 0)]
    END as hit_array,
    CASE 
        WHEN y.unique_visitors_array IS NULL THEN 
            ARRAY_FILL(0::BIGINT, ARRAY[t.day_of_month - 1]) || ARRAY[t.unique_visitors]
        ELSE 
            y.unique_visitors_array || ARRAY[COALESCE(t.unique_visitors, 0)]
    END as unique_visitors_array
FROM yesterday y
FULL OUTER JOIN daily_position t ON y.host = t.host;
```

**Explanation**: This query builds monthly arrays day by day[8][9][10]. For new months, it creates arrays with zeros for previous days and the actual value for the current day. For existing months, it appends the new day's metrics to the existing arrays.

## Key Learning Points for Data Modeling

1. **Incremental Processing**: All these queries follow incremental patterns to avoid reprocessing historical data, which is crucial for performance at scale.

2. **Storage vs. Performance Trade-offs**: Cumulative tables use more storage but provide faster queries. Datelist integers use less storage but require more complex logic.

3. **Data Structure Choice**: Arrays, maps, and integers each serve different purposes. Choose based on your query patterns and performance requirements.

4. **Deduplication Strategy**: Always understand your business keys before deduplicating. The wrong deduplication logic can corrupt your data.

5. **Full Outer Joins**: This pattern is fundamental in cumulative table design for handling both new and existing entities.

These patterns form the foundation of modern data warehousing and are essential for building scalable analytics systems[11].

[1] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/da2509a8-1b02-41e0-b505-5399ddbccec3/anaylze_datelist.sql
[2] https://www.restack.io/p/automated-data-cleaning-answer-sql-queries-data-deduplication-cat-ai
[3] https://stackoverflow.com/questions/6471463/how-to-delete-duplicates-in-sql-table-based-on-multiple-fields
[4] https://github.com/DataExpert-io/cumulative-table-design/blob/master/README.md
[5] https://www.linkedin.com/pulse/datelist-int-efficient-data-structure-user-growth-max-sung
[6] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/4846624b-07ee-4020-8de6-6e7e31d6cfd2/user_cumulated_populate.sql
[7] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/99fb3735-e772-467a-981c-d1c9923f5501/generate_datelist.sql
[8] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/a252d5d6-d47b-4b0f-b855-1136aa206338/array_metrics_analysis.sql
[9] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/a878f501-2043-4024-9bfc-83f98ba1f09e/generate_monthly_array_metrics.sql
[10] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/f4c199a9-bfec-4aa9-b30e-d20fba76e3d5/quick_sum_device_hits.sql
[11] programming.data_engineering
[12] https://github.com/alifradi/Data-Engineering-track-with-Zachary
[13] https://stackoverflow.com/questions/72807776/how-do-i-remove-duplicates-using-delete-from-and-a-sub-query/72808124
[14] https://www.youtube.com/watch?v=h48xzQR3wNQ
[15] https://dev.to/kellyblaire/mastering-data-definition-language-ddl-statements-in-sql-1c8m
[16] https://www.datacamp.com/tutorial/sql-ddl-commands
[17] https://help.ivanti.com/ht/help/en_US/IES/86/UG_DC/work-with-device-event.htm
[18] https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-2016
[19] https://docs.singlestore.com/cloud/reference/sql-reference/conditional-functions/cast-or-convert/
[20] https://mathematica.stackexchange.com/questions/181196/how-to-convert-datelist-format-to-standard-sql-format/181297
[21] https://bowtiedraptor.substack.com/p/sql-8-data-manipulation-and-conversion
[22] https://mariadb.com/kb/en/ddl-statements-that-differ-for-columnstore/
[23] https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table
[24] https://www.tobikodata.com/blog/correctly-loading-incremental-data-at-scale
[25] https://experienceleague.adobe.com/en/docs/experience-platform/query/key-concepts/incremental-load
[26] https://stackoverflow.com/questions/50595080/how-to-do-incremental-loading-in-sql-server
[27] https://stackoverflow.com/questions/3695369/sql-how-to-remove-duplicates-within-select-query
[28] https://www.datacamp.com/tutorial/sql-remove-duplicates
[29] https://www.reddit.com/r/SQL/comments/r3aop3/sql_delete_duplicates/
[30] https://www.ituonline.com/blogs/distinct-sql/
[31] https://www.datamesh-architecture.com/howto/deduplication
[32] https://morioh.com/a/c659090b339d/3-ways-to-remove-duplicates-data-in-sql
[33] https://github.com/DataExpert-io/data-engineer-handbook/blob/main/bootcamp/materials/2-fact-data-modeling/homework/homework.md
[34] https://stackoverflow.com/questions/64475717/how-to-map-java-kotlin-string-array-and-postgres-sql-array-with-jpa-and-hibernat
[35] https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language
[36] https://www.eecs.yorku.ca/~papaggel/courses/eecs3421/docs/lectures/05-dml-ddl-views-indexes.pdf
[37] https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language
[38] https://github.com/DataExpert-io/cumulative-table-design/blob/master/queries/active_users_cumulated_populate.sql
[39] https://dba.stackexchange.com/questions/153847/schema-design-for-user-activity-logging
[40] https://github.com/DataExpert-io/cumulative-table-design/blob/master/tables/active_users_cumulated.sql
[41] https://stackoverflow.com/questions/2120544/how-to-get-cumulative-sum
[42] https://www.interviewquery.com/p/sql-cumulative-sum-guide
[43] https://blogs.oracle.com/sql/post/cumulative-running-total-of-previous-rows-with-sql
[44] https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-stream-analytics-query-patterns
[45] https://community.smartthings.com/t/retrieving-list-of-events/101440
[46] https://stackoverflow.com/questions/23020771/mysql-aggregating-data-from-list-of-events-query-optimization
[47] https://search.r-project.org/CRAN/refmans/cumulocityr/html/get_events.html
[48] https://www.stratascratch.com/blog/computing-cumulative-sum-in-sql-made-easy/
[49] https://www.w3schools.com/sql/func_sqlserver_convert.asp
[50] https://www.mssqltips.com/sqlservertip/6874/sql-cast-function-for-data-type-conversions/
[51] https://www.mssqltips.com/sqlservertip/8004/sql-convert-examples-dates-integers-strings/
[52] https://docs.citusdata.com/en/v11.1/develop/reference_ddl.html
[53] https://stackoverflow.com/questions/44326669/how-to-get-a-ddl-of-an-existing-table-using-apache-pheonix
[54] https://forums.oracle.com/ords/apexds/post/view-ddl-sql-tab-4501
[55] https://help.claris.com/en/pro-help/content/gettableddl.html
[56] https://www.ibm.com/support/pages/database-table-schema-0
[57] https://github.com/ActivitySchema/ActivitySchema/blob/main/1.1.md
[58] https://cloud.google.com/dataform/docs/reference/sample-scripts
[59] https://stackoverflow.com/questions/16555454/how-to-generate-auto-increment-field-in-select-query
[60] https://learn.microsoft.com/en-us/fabric/data-factory/dataflow-gen2-incremental-refresh
[61] https://hudi.apache.org/docs/quick-start-guide/
[62] https://stackoverflow.com/questions/8745179/update-a-list-in-sql-with-incremental-dates-using-one-statement
[63] https://www.youtube.com/watch?v=VNeDR82c0qQ
[64] https://docs.squiz.net/funnelback/docs/latest/reference/configuration-options/collection-options/db.incremental_sql_query.html
[65] https://stackoverflow.com/questions/68819496/i-am-trying-to-get-a-list-of-unique-values-from-a-2d-array-and-multiple-columns
[66] https://stackoverflow.com/questions/62174469/how-to-get-unique-values-from-a-column-of-arrays-postgresql
[67] https://www.youtube.com/watch?v=tp0jLnmKcEs
[68] https://www.youtube.com/watch?v=sG-QY0JZpyY
[69] https://dev.mysql.com/doc/mysql-perfschema-excerpt/5.7/en/performance-schema-hosts-table.html
[70] https://www.usna.edu/Users/cs/adina/teaching/it360/spring2014/slides/it360_Set7_SQL_DDLandDML.pdf
[71] https://oercommons.org/authoring/21861-data-warehouse-design-using-oracle/3/view
[72] https://community.tableau.com/s/question/0D54T00000C66FpSAJ/does-incremental-refresh-reduce-load-on-datasource
[73] https://experienceleague.adobe.com/en/docs/campaign-web/v8/wf/design-workflows/incremental-query
[74] https://stackoverflow.com/questions/78245632/how-to-resolve-this-incremental-loading-with-sql
[75] https://learn.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-overview
[76] https://experienceleague.adobe.com/en/docs/campaign-classic/using/automating-with-workflows/targeting-activities/incremental-query
[77] https://hevodata.com/learn/etl-incremental/
[78] https://community.qlik.com/t5/QlikView-App-Dev/Incremental-Load-on-SQL-Joined-Tables/td-p/1193332
[79] https://www.clicdata.com/blog/incremental-data-loads/
