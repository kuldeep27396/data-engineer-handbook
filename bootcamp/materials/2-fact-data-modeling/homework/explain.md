I'll show you the before and after query outputs for each of the SQL files to help you understand how these data modeling patterns work in practice.

## Datelist Integer Analysis (analyze_datelist.sql)

### **Before State - users_cumulated table**
```sql
user_id | dates_active                                    | date
--------|------------------------------------------------|------------
1001    | ['2023-03-01', '2023-03-03', '2023-03-15']    | 2023-03-31
1002    | ['2023-03-29', '2023-03-30', '2023-03-31']    | 2023-03-31
1003    | ['2023-03-01', '2023-03-02', '2023-03-03']    | 2023-03-31
```

### **After Query Execution**
```sql
user_id | datelist_int                      | monthly_active | l32 | weekly_active | l7 | weekly_active_previous_week
--------|-----------------------------------|----------------|-----|---------------|----|--------------------------
1001    | 10000000000000100000000000000000 | true           | 3   | false         | 0  | false
1002    | 00000000000000000000000000000111 | true           | 3   | true          | 3  | false
1003    | 11100000000000000000000000000000 | true           | 3   | false         | 0  | false
```

**Explanation**: The datelist_int converts date arrays into 32-bit integers where each bit represents a day. User 1002 shows recent activity (last 3 days), while users 1001 and 1003 have older activity patterns[1].

## Array Metrics Analysis (array_metrics_analysis.sql)

### **Before State - events table**
```sql
user_id | event_time           | event_type
--------|---------------------|------------
1001    | 2023-01-01 10:30:00 | page_view
1001    | 2023-01-01 11:45:00 | page_view
1002    | 2023-01-01 09:15:00 | page_view
1003    | 2023-01-01 14:20:00 | page_view
1003    | 2023-01-01 15:30:00 | page_view
1003    | 2023-01-01 16:45:00 | page_view
```

### **After Daily Aggregation**
```sql
user_id | date       | num_site_hits
--------|------------|---------------
1001    | 2023-01-01 | 2
1002    | 2023-01-01 | 1
1003    | 2023-01-01 | 3
```

### **After Array Metrics Insert**
```sql
user_id | month_start | metric_name | metric_array
--------|-------------|-------------|-------------
1001    | 2023-01-01  | site_hits   | [2]
1002    | 2023-01-01  | site_hits   | [1]
1003    | 2023-01-01  | site_hits   | [3]
```

### **After Multiple Days (Day 3)**
```sql
user_id | month_start | metric_name | metric_array
--------|-------------|-------------|-------------
1001    | 2023-01-01  | site_hits   | [2, 0, 5]
1002    | 2023-01-01  | site_hits   | [1, 3, 0]
1003    | 2023-01-01  | site_hits   | [3, 1, 2]
```

**Explanation**: Each array position represents a day of the month. User 1001 had 2 hits on day 1, 0 on day 2, and 5 on day 3[2].

## Generate Datelist (generate_datelist.sql)

### **Before State - users_cumulated**
```sql
user_id | dates_active                                    | date
--------|------------------------------------------------|------------
1001    | ['2023-03-01', '2023-03-15', '2023-03-30']    | 2023-03-31
1002    | ['2023-03-29', '2023-03-30', '2023-03-31']    | 2023-03-31
```

### **After Starter CTE**
```sql
user_id | valid_date | is_active | days_since
--------|------------|-----------|------------
1001    | 2023-03-01 | true      | 30
1001    | 2023-03-02 | false     | 29
1001    | 2023-03-15 | true      | 16
1001    | 2023-03-30 | true      | 1
1002    | 2023-03-29 | true      | 2
1002    | 2023-03-30 | true      | 1
1002    | 2023-03-31 | true      | 0
```

### **After Bits CTE and Final Insert**
```sql
user_id | datelist_int                      | date
--------|-----------------------------------|------------
1001    | 10000000000000100000000000000010 | 2023-03-31
1002    | 00000000000000000000000000000111 | 2023-03-31
```

**Explanation**: The POW(2, 32 - days_since) calculation creates bit positions. User 1002's binary shows activity on the last 3 days (rightmost 3 bits = 111)[3].

## Monthly Array Metrics (generate_monthly_array_metrics.sql)

### **Before State - Day 2**
```sql
user_id | hit_array | month_start | first_found_date | date_partition
--------|-----------|-------------|------------------|----------------
1001    | [5, 3]    | 2023-03-01  | 2023-03-01      | 2023-03-02
1002    | [2, 0]    | 2023-03-01  | 2023-03-01      | 2023-03-02
```

### **Today's Events (Day 3)**
```sql
user_id | today_date | num_hits
--------|------------|----------
1001    | 2023-03-03 | 7
1003    | 2023-03-03 | 4
```

### **After Insert (Day 3)**
```sql
user_id | hit_array   | month_start | first_found_date | date_partition
--------|-------------|-------------|------------------|----------------
1001    | [5, 3, 7]   | 2023-03-01  | 2023-03-01      | 2023-03-03
1002    | [2, 0, 0]   | 2023-03-01  | 2023-03-01      | 2023-03-03
1003    | [NULL, NULL, 4] | 2023-03-01  | 2023-03-03      | 2023-03-03
```

**Explanation**: User 1001 gets 7 hits appended to existing array. User 1002 gets 0 appended (no activity). User 1003 is new, so gets NULL padding for missed days[4].

## Quick Sum Analysis (quick_sum_device_hits.sql)

### **Before State - monthly_user_site_hits**
```sql
user_id | hit_array     | month_start | date_partition
--------|---------------|-------------|----------------
1001    | [5, 3, 7]     | 2023-03-01  | 2023-03-03
1002    | [2, 0, 0]     | 2023-03-01  | 2023-03-03
1003    | [NULL, NULL, 4] | 2023-03-01  | 2023-03-03
1004    | [1, 8, 2]     | 2023-03-01  | 2023-03-03
```

### **After Aggregation Query**
```sql
month_start | num_hits_mar_1 | num_hits_mar_2
------------|----------------|----------------
2023-03-01  | 8              | 11
```

**Explanation**: SUM(hit_array[1]) = 5+2+0+1 = 8 total hits on March 1st. SUM(hit_array[2]) = 3+0+0+8 = 11 total hits on March 2nd. NULL values are ignored in SUM[5].

## User Cumulated Population (user_cumulated_populate.sql)

### **Before State - Day 30**
```sql
user_id | dates_active                           | date
--------|----------------------------------------|------------
1001    | ['2023-03-01', '2023-03-15', '2023-29'] | 2023-03-30
1002    | ['2023-03-28', '2023-03-29']           | 2023-03-30
```

### **Today's Activity (Day 31)**
```sql
user_id | today_date | num_events
--------|------------|------------
1001    | 2023-03-31 | 15
1003    | 2023-03-31 | 8
```

### **After FULL OUTER JOIN and Insert**
```sql
user_id | dates_active                                      | date
--------|---------------------------------------------------|------------
1001    | ['2023-03-01', '2023-03-15', '2023-03-29', '2023-03-31'] | 2023-03-31
1002    | ['2023-03-28', '2023-03-29']                      | 2023-03-31
1003    | ['2023-03-31']                                    | 2023-03-31
```

**Explanation**: User 1001 gets new date appended to existing array. User 1002 has no activity, so gets yesterday's data with incremented date. User 1003 is new and gets a fresh array with today's date[6].

## Key Patterns Demonstrated

**Incremental Processing**: Each query shows how to handle both existing users (yesterday) and new activity (today) without losing historical data.

**Array Growth**: Arrays grow day by day, with each position representing a specific time period.

**NULL Handling**: COALESCE and CASE statements manage missing data gracefully.

**Bit Manipulation**: Datelist integers provide extremely compact storage for activity patterns.

**Aggregation Efficiency**: Pre-computed arrays enable fast daily/monthly reporting without scanning raw events.

These patterns are fundamental to building scalable data warehouses that can handle millions of users and billions of events efficiently.

[1] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/da2509a8-1b02-41e0-b505-5399ddbccec3/anaylze_datelist.sql
[2] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/a252d5d6-d47b-4b0f-b855-1136aa206338/array_metrics_analysis.sql
[3] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/99fb3735-e772-467a-981c-d1c9923f5501/generate_datelist.sql
[4] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/a878f501-2043-4024-9bfc-83f98ba1f09e/generate_monthly_array_metrics.sql
[5] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/f4c199a9-bfec-4aa9-b30e-d20fba76e3d5/quick_sum_device_hits.sql
[6] https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/74673586/4846624b-07ee-4020-8de6-6e7e31d6cfd2/user_cumulated_populate.sql
[7] https://imply.io/blog/upserts-and-data-deduplication-with-druid/
[8] https://www.stratascratch.com/blog/computing-cumulative-sum-in-sql-made-easy/
[9] https://experienceleague.adobe.com/en/docs/campaign-web/v8/wf/design-workflows/incremental-query
[10] https://learn.microsoft.com/en-us/power-query/dataflows/incremental-refresh
[11] https://stackoverflow.com/questions/39150069/how-can-i-exclude-duplicate-records-from-sql-query
[12] https://www.reddit.com/r/PostgreSQL/comments/1hckhkz/data_deduplication/
[13] https://community.influxdata.com/t/query-issue-cumulative-values-to-simple-values-in-influxdb/30044
[14] https://learn.microsoft.com/en-us/answers/questions/788170/cumulative-and-rollup-of-the-data-in-sql-server
[15] https://community.fabric.microsoft.com/t5/Service/Custom-query-Incremental-Refresh/m-p/3231249
[16] https://stackoverflow.com/questions/510121/reset-autoincrement-in-sql-server-after-delete
[17] https://www.timmitchell.net/post/2016/01/20/using-sql-server-change-tracking-for-incremental-loads/
[18] https://stackoverflow.com/questions/42226798/remove-duplicate-data-from-query-results
[19] https://stepzen.com/blog/graphql-optimization-part2-deduplication-reuse
[20] https://community.grafana.com/t/how-to-use-exact-de-duplication-on-query-result/111086
[21] https://www.youtube.com/watch?v=tIjselZbU-A
[22] https://dedupe-staging.readthedocs.io/en/stable/API-documentation.html
[23] https://stackoverflow.com/questions/57008730/how-to-remove-duplicates-in-my-query-result
[24] https://stats.stackexchange.com/questions/392216/removing-duplicates-before-train-test-split
[25] https://stackoverflow.com/questions/2120544/how-to-get-cumulative-sum
[26] https://stackoverflow.com/questions/67783781/calculating-the-cumulative-sum-with-a-specific-date-merged-to-single-column-in
[27] https://www.reddit.com/r/SQL/comments/11nx57u/cumulative_sum_or_running_sum/
[28] https://github.com/DataExpert-io/data-engineer-handbook/blob/main/bootcamp/materials/2-fact-data-modeling/homework/homework.md
[29] https://coffingdw.com/sql-server-analytics-cumulative-sum/
[30] https://github.com/alifradi/Data-Engineering-track-with-Zachary
[31] https://community.fabric.microsoft.com/t5/Desktop/Cumulative-Daily-Volume-Measure-in-Power-BI-not-working-as/m-p/3738249
[32] https://experienceleague.adobe.com/en/docs/campaign/automation/workflows/wf-activities/targeting-activities/incremental-query
[33] https://learn.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-configure
[34] https://developerdocs.instructure.com/services/dap/dap-cli-readme/dap-cli-reference/dap-cli-reference-incremental
[35] https://blog.det.life/i-completed-a-senior-data-engineer-code-challenge-for-fun-and-this-is-how-it-went-part-ii-3af62f4b982e?gi=01aeee443835
[36] https://www.googlecloudcommunity.com/gc/Data-Analytics/Dataform-incremental-always-returns-false/m-p/702603
[37] https://docs.matillion.com/data-productivity-cloud/designer/docs/incremental-load-example/
[38] https://experienceleaguecommunities.adobe.com/t5/adobe-campaign-standard/difference-between-a-query-and-incremental-query/m-p/234905
[39] https://stackoverflow.com/questions/30808156/using-sql-can-i-get-incremental-changes-in-data-from-query-results-loops
[40] https://people.eecs.berkeley.edu/~totemtang/paper/InQP.pdf
[41] https://www.youtube.com/watch?v=QiCTVKLVfBE
[42] https://docs.cambridgesemantics.com/anzo/v2025.0/userdoc/gdi-incremental.htm
[43] https://impala.apache.org/docs/build/html/topics/impala_compute_stats.html
