#========================================
#COMPREHENSIVE TIME-PERIOD IMPLEMENTATIONS
#========================================

#========================================
#1. SEASONAL ANALYSIS (Back-to-School vs Holiday Periods)
#========================================

WITH seasonal_periods AS (
SELECT
    date,
    CASE
        WHEN MONTH(date) IN (9, 10) THEN ‘Back-to-School’
        WHEN MONTH(date) IN (11, 12) THEN ‘Fall/Winter Break’
        WHEN MONTH(date) IN (1, 2) THEN ‘Winter/Spring’
        WHEN MONTH(date) IN (3, 4) THEN ‘Spring Break Period’
        WHEN MONTH(date) IN (5, 6) THEN ‘End of Year’
            ELSE ‘Summer’
    END as season,
    CASE
        WHEN date IN (
    ‘2024-11-28’, ‘2024-11-29’, ‘2024-12-23’, ‘2024-12-24’,
    ‘2024-12-25’, ‘2024-12-26’, ‘2024-12-27’, ‘2024-12-30’,
    ‘2024-12-31’, ‘2025-01-01’) THEN ‘Holiday’
        WHEN DAYOFWEEK(date) IN (1, 7) THEN ‘Weekend’
            ELSE ‘Regular School Day’
    END as day_type
FROM attendance_events
WHERE date >= ‘2024-09-01’ AND date <= ‘2025-06-30’
GROUP BY date
),

seasonal_attendance AS (
SELECT
    sp.season,
    sp.day_type,
    s.grade_level,
    s.hometown as school,
    COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) as present_count,
    COUNT(*) as total_count,
    ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as attendance_rate
FROM seasonal_periods sp
JOIN attendance_events ae 
    ON sp.date = ae.date
JOIN all_students s 
    ON ae.student_id = s.student_id
WHERE sp.day_type = ‘Regular School Day’
GROUP BY sp.season, sp.day_type, s.grade_level, s.hometown
),

seasonal_summary AS (
SELECT
    season,
    grade_level,
    school,
    AVG(attendance_rate) as avg_seasonal_rate,
    STDDEV(attendance_rate) as seasonal_volatility,
    COUNT(*) as days_in_season
FROM seasonal_attendance
GROUP BY season, grade_level, school
)

SELECT
    school,
    grade_level,
    season,
    ROUND(avg_seasonal_rate, 2) as seasonal_attendance_rate,
    ROUND(seasonal_volatility, 2) as volatility,
    days_in_season,
    CASE
        WHEN avg_seasonal_rate = MAX(avg_seasonal_rate) OVER (PARTITION BY school, grade_level)
            THEN ‘BEST SEASON’
        WHEN avg_seasonal_rate = MIN(avg_seasonal_rate) OVER (PARTITION BY school, grade_level)
            THEN ‘WORST SEASON’
        ELSE ‘AVERAGE’
            END as seasonal_performance
FROM seasonal_summary
ORDER BY school, grade_level, avg_seasonal_rate DESC;

#========================================
#2. DAY-OF-WEEK PATTERN ANALYSIS
#========================================

WITH daily_patterns AS (
SELECT
    s.hometown as school,
    s.grade_level,
    DAYOFWEEK(ae.date) as day_of_week,
    CASE
        WHEN DAYOFWEEK(ae.date) = 2 THEN ‘Monday’
        WHEN DAYOFWEEK(ae.date) = 3 THEN ‘Tuesday’
        WHEN DAYOFWEEK(ae.date) = 4 THEN ‘Wednesday’
        WHEN DAYOFWEEK(ae.date) = 5 THEN ‘Thursday’
        WHEN DAYOFWEEK(ae.date) = 6 THEN ‘Friday’
        ELSE ‘Weekend’
            END as day_name,
    ae.date,
    COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) as daily_present,
    COUNT(*) as daily_total,
    ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as daily_rate
FROM all_students s
JOIN attendance_events ae 
    ON s.student_id = ae.student_id
WHERE ae.date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
    AND DAYOFWEEK(ae.date) BETWEEN 2 AND 6 – Monday to Friday only
GROUP BY s.hometown, s.grade_level, DAYOFWEEK(ae.date), day_name, ae.date
),

weekly_patterns AS (
SELECT
    school,
    grade_level,
    day_name,
    day_of_week,
    AVG(daily_rate) as avg_day_rate,
    STDDEV(daily_rate) as day_volatility,
    COUNT(*) as total_days_analyzed,
    MIN(daily_rate) as worst_day_rate,
    MAX(daily_rate) as best_day_rate
FROM daily_patterns
GROUP BY school, grade_level, day_name, day_of_week
),

school_weekly_analysis AS (
SELECT
    school,
    grade_level,
    AVG(CASE WHEN day_name = ‘Monday’ THEN avg_day_rate END) as monday_avg,
    AVG(CASE WHEN day_name = ‘Tuesday’ THEN avg_day_rate END) as tuesday_avg,
    AVG(CASE WHEN day_name = ‘Wednesday’ THEN avg_day_rate END) as wednesday_avg,
    AVG(CASE WHEN day_name = ‘Thursday’ THEN avg_day_rate END) as thursday_avg,
    AVG(CASE WHEN day_name = ‘Friday’ THEN avg_day_rate END) as friday_avg,
    – Calculate weekly patterns
    (AVG(CASE WHEN day_name = ‘Monday’ THEN avg_day_rate END) -
    AVG(CASE WHEN day_name = ‘Friday’ THEN avg_day_rate END)) as monday_friday_gap,
    AVG(avg_day_rate) as overall_weekly_avg
FROM weekly_patterns
GROUP BY school, grade_level
)

SELECT
    school,
    grade_level,
    ROUND(monday_avg, 2) as monday_rate,
    ROUND(tuesday_avg, 2) as tuesday_rate,
    ROUND(wednesday_avg, 2) as wednesday_rate,
    ROUND(thursday_avg, 2) as thursday_rate,
    ROUND(friday_avg, 2) as friday_rate,
    ROUND(monday_friday_gap, 2) as monday_friday_difference,
    ROUND(overall_weekly_avg, 2) as weekly_average,
    CASE
    WHEN monday_friday_gap > 10 THEN ‘SEVERE Monday Problem’
    WHEN monday_friday_gap > 5 THEN ‘Moderate Monday Problem’
    WHEN monday_friday_gap < -10 THEN ‘SEVERE Friday Problem’
    WHEN monday_friday_gap < -5 THEN ‘Moderate Friday Problem’
    ELSE ‘Consistent Week’
    END as weekly_pattern_diagnosis
FROM school_weekly_analysis
ORDER BY monday_friday_gap DESC;

#========================================
#3. ROLLING TREND ANALYSIS WITH ALERTS
#========================================

WITH rolling_attendance AS (
SELECT
    s.hometown as school,
    s.grade_level,
    ae.date,
    COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) as daily_present,
    COUNT(*) as daily_total,
    ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as daily_rate,
    #7-day rolling average
    ROUND(
        AVG(COUNT(
            CASE WHEN ae.attendance = 1 THEN 1 END
            ) * 100.0 / COUNT(*))
        OVER (PARTITION BY s.hometown, s.grade_level ORDER BY ae.date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as rolling_7day_avg,

    #30-day rolling average
    ROUND(
        AVG(COUNT(
            CASE WHEN ae.attendance = 1 THEN 1 END
            ) * 100.0 / COUNT(*))
        OVER (PARTITION BY s.hometown, s.grade_level ORDER BY ae.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) as rolling_30day_avg,

    # Previous period comparison
    LAG(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 7)
    OVER (PARTITION BY s.hometown, s.grade_level ORDER BY ae.date) as rate_7days_ago
FROM all_students s
JOIN attendance_events ae 
    ON s.student_id = ae.student_id
WHERE ae.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY s.hometown, s.grade_level, ae.date
),

trend_analysis AS (
SELECT
    school,
    grade_level,
    date,
    daily_rate,
    rolling_7day_avg,
    rolling_30day_avg,
    rate_7days_ago,
    (rolling_7day_avg - rolling_30day_avg) as short_vs_long_trend,
    (daily_rate - rate_7days_ago) as week_over_week_change,
    CASE
        WHEN rolling_7day_avg < 75 THEN ‘CRITICAL’
        WHEN rolling_7day_avg < 85 THEN ‘WARNING’
        WHEN rolling_7day_avg < 90 THEN ‘WATCH’
    ELSE ‘HEALTHY’
    END as alert_level,
    ROW_NUMBER() OVER (PARTITION BY school, grade_level ORDER BY date DESC) as recency_rank
FROM rolling_attendance
WHERE rolling_30day_avg IS NOT NULL
)

SELECT
    school,
    grade_level,
    date as latest_date,
    daily_rate as latest_daily_rate,
    rolling_7day_avg as current_7day_trend,
    rolling_30day_avg as current_30day_baseline,
    ROUND(short_vs_long_trend, 2) as trend_direction,
    ROUND(week_over_week_change, 2) as weekly_change,
    alert_level,
    CASE
        WHEN short_vs_long_trend > 2 THEN ‘IMPROVING’
        WHEN short_vs_long_trend < -2 THEN ‘DECLINING’
            ELSE ‘STABLE’
        END as trend_status,
    CASE
        WHEN ABS(week_over_week_change) > 15 THEN ‘HIGH VOLATILITY’
        WHEN ABS(week_over_week_change) > 8 THEN ‘MODERATE VOLATILITY’
            ELSE ‘LOW VOLATILITY’
        END as volatility_level
FROM trend_analysis
WHERE recency_rank = 1 – Most recent data only
ORDER BY
    CASE alert_level
        WHEN ‘CRITICAL’ THEN 1
        WHEN ‘WARNING’ THEN 2
        WHEN ‘WATCH’ THEN 3
            ELSE 4
        END,
    short_vs_long_trend ASC;

#========================================
# 4. COHORT ANALYSIS (Monthly Enrollment Groups)
# ========================================

WITH student_cohorts AS (
SELECT
    s.student_id,
    s.grade_level,
    s.hometown as school,
    – Determine enrollment cohort (assuming date_of_birth correlates with enrollment timing)
    CASE
        WHEN MONTH(s.date_of_birth) BETWEEN 9 AND 12 THEN ‘Fall Enrollees’
        WHEN MONTH(s.date_of_birth) BETWEEN 1 AND 3 THEN ‘Winter Enrollees’
        WHEN MONTH(s.date_of_birth) BETWEEN 4 AND 6 THEN ‘Spring Enrollees’
        ELSE ‘Summer Enrollees’
            END as enrollment_cohort,
    – Age-based cohort
    CASE
    WHEN DATEDIFF(CURDATE(), s.date_of_birth) / 365 < 14 THEN ‘Younger Students’
    WHEN DATEDIFF(CURDATE(), s.date_of_birth) / 365 > 18 THEN ‘Older Students’
    ELSE ‘Typical Age’
    END as age_cohort
FROM all_students s
),
cohort_performance AS (
SELECT
sc.school,
sc.grade_level,
sc.enrollment_cohort,
sc.age_cohort,
COUNT(DISTINCT sc.student_id) as cohort_size,
COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) as total_present,
COUNT(*) as total_possible,
ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as cohort_attendance_rate,
– Calculate consistency
STDDEV(CASE WHEN ae.attendance = 1 THEN 100.0 ELSE 0.0 END) as individual_consistency
FROM student_cohorts sc
JOIN attendance_events ae ON sc.student_id = ae.student_id
WHERE ae.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY sc.school, sc.grade_level, sc.enrollment_cohort, sc.age_cohort
)
SELECT
school,
grade_level,
enrollment_cohort,
age_cohort,
cohort_size,
cohort_attendance_rate,
ROUND(individual_consistency, 2) as consistency_score,
CASE
WHEN cohort_attendance_rate = MAX(cohort_attendance_rate) OVER (PARTITION BY school, grade_level)
THEN ‘TOP PERFORMING COHORT’
WHEN cohort_attendance_rate = MIN(cohort_attendance_rate) OVER (PARTITION BY school, grade_level)
THEN ‘NEEDS ATTENTION’
ELSE ‘AVERAGE’
END as cohort_status,
CASE
WHEN individual_consistency < 20 THEN ‘Very Consistent’
WHEN individual_consistency < 35 THEN ‘Moderately Consistent’
ELSE ‘Highly Variable’
END as consistency_level
FROM cohort_performance
WHERE cohort_size >= 5 – Only analyze cohorts with sufficient size
ORDER BY school, grade_level, cohort_attendance_rate DESC;

#========================================
# 5. PERFORMANCE OPTIMIZATION STRATEGIES
#========================================

#A. Create materialized views for frequently accessed aggregations
CREATE VIEW mv_daily_school_attendance AS
SELECT
    s.hometown as school,
    s.grade_level,
    ae.date,
    COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) as present_count,
    COUNT(*) as total_count,
    ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as attendance_rate,
    DAYOFWEEK(ae.date) as day_of_week,
    MONTH(ae.date) as month_num,
    YEAR(ae.date) as year_num
FROM all_students s
JOIN attendance_events ae ON s.student_id = ae.student_id
WHERE ae.date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
GROUP BY s.hometown, s.grade_level, ae.date;

#B. Partitioning strategy for very large datasets
/*
– Partition attendance_events by year and month for optimal performance
ALTER TABLE attendance_events
PARTITION BY RANGE (YEAR(date) * 100 + MONTH(date)) (
PARTITION p202401 VALUES LESS THAN (202402),
PARTITION p202402 VALUES LESS THAN (202403),
PARTITION p202403 VALUES LESS THAN (202404),
– … continue for all months
PARTITION p202412 VALUES LESS THAN (202501),
PARTITION p202501 VALUES LESS THAN (202502)
– Add new partitions as needed
);
*/

# C. Summary table for fast dashboard queries
CREATE TABLE attendance_summary (
school VARCHAR(100),
grade_level INT,
analysis_date DATE,
period_type VARCHAR(20), – ‘daily’, ‘weekly’, ‘monthly’
attendance_rate DECIMAL(5,2),
present_count INT,
total_count INT,
volatility DECIMAL(5,2),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (school, grade_level, analysis_date, period_type)
);

# D. Optimized query patterns for large datasets
# Use this pattern for better performance on large tables:
WITH filtered_data AS (
SELECT
s.student_id,
s.hometown,
s.grade_level,
ae.date,
ae.attendance
FROM all_students s
JOIN attendance_events ae ON s.student_id = ae.student_id
WHERE ae.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
AND ae.date < CURDATE()
#Add any additional filters here first
)
SELECT
hometown as school,
grade_level,
COUNT(CASE WHEN attendance = 1 THEN 1 END) as present,
COUNT(*) as total,
ROUND(COUNT(CASE WHEN attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as rate
FROM filtered_data
GROUP BY hometown, grade_level
ORDER BY rate DESC;

#========================================
# 6. AUTOMATED ALERT SYSTEM
#========================================

– Query to identify schools/grades needing immediate attention
WITH alert_conditions AS (
SELECT
school,
grade_level,
current_7day_trend,
current_30day_baseline,
trend_direction,
weekly_change,
alert_level,
CASE
WHEN alert_level = ‘CRITICAL’ AND trend_direction < -5 THEN ‘URGENT: Declining Critical’
WHEN alert_level = ‘WARNING’ AND ABS(weekly_change) > 10 THEN ‘HIGH: Volatile Warning’
WHEN alert_level = ‘WATCH’ AND trend_direction < -3 THEN ‘MEDIUM: Declining Watch’
ELSE ‘LOW: Monitor’
END as priority_level,
CASE
WHEN alert_level = ‘CRITICAL’ THEN ‘Principal + Superintendent’
WHEN alert_level = ‘WARNING’ THEN ‘Principal + Counselor’
WHEN alert_level = ‘WATCH’ THEN ‘Teacher + Counselor’
ELSE ‘Teacher’
END as notification_level
FROM (
– Insert the trend_analysis CTE results here
SELECT
school,
grade_level,
rolling_7day_avg as current_7day_trend,
rolling_30day_avg as current_30day_baseline,
short_vs_long_trend as trend_direction,
week_over_week_change as weekly_change,
alert_level
FROM trend_analysis
WHERE recency_rank = 1
) latest_trends
)
SELECT
priority_level,
notification_level,
school,
grade_level,
ROUND(current_7day_trend, 2) as current_rate,
ROUND(trend_direction, 2) as trend,
ROUND(weekly_change, 2) as weekly_change,
CONCAT(
’ALERT: ’, school, ’ Grade ’, grade_level,
’ has ’, ROUND(current_7day_trend, 1), ’% attendance ’,
CASE WHEN trend_direction < 0 THEN ‘(DECLINING)’ ELSE ‘(STABLE/IMPROVING)’ END
) as alert_message
FROM alert_conditions
WHERE priority_level != ‘LOW: Monitor’
ORDER BY
CASE priority_level
WHEN ‘URGENT: Declining Critical’ THEN 1
WHEN ‘HIGH: Volatile Warning’ THEN 2
WHEN ‘MEDIUM: Declining Watch’ THEN 3
ELSE 4
END,
current_7day_trend ASC;









-------------------------------------------------------------------------------
-- ========================================
-- SAMPLE RESULTS WALKTHROUGH
-- ========================================

-- Sample Data for 60 days of attendance (simplified)
-- This shows what your results would look like:

-- ========================================
-- 1. SEASONAL ANALYSIS SAMPLE RESULTS
-- ========================================

-- Expected Output:
/*
school          | grade_level | season           | seasonal_attendance_rate | volatility | seasonal_performance
Lincoln High    | 9          | Back-to-School   | 94.2                    | 3.1        | BEST SEASON
Lincoln High    | 9          | Fall/Winter Break| 87.8                    | 5.4        | WORST SEASON
Lincoln High    | 9          | Winter/Spring    | 91.3                    | 4.2        | AVERAGE
Washington High | 10         | Back-to-School   | 92.1                    | 2.8        | BEST SEASON
Washington High | 10         | Fall/Winter Break| 85.6                    | 6.1        | WORST SEASON
*/

-- Key Insights:
-- • All schools show highest attendance in Back-to-School period
-- • Fall/Winter Break period consistently has lowest attendance
-- • Volatility increases during holiday periods (higher standard deviation)

-- ========================================
-- 2. DAY-OF-WEEK PATTERN ANALYSIS SAMPLE RESULTS
-- ========================================

-- Expected Output:
/*
school          | grade_level | monday_rate | tuesday_rate | wednesday_rate | thursday_rate | friday_rate | monday_friday_difference | weekly_pattern_diagnosis
Lincoln High    | 9          | 82.4        | 89.1         | 91.2          | 88.7         | 85.3        | -2.9                    | Moderate Friday Problem
Washington High | 10         | 78.2        | 87.8         | 89.4          | 87.1         | 84.6        | -6.4                    | Moderate Friday Problem  
Jefferson High  | 11         | 85.1        | 88.2         | 89.7          | 87.9         | 75.3        | 9.8                     | Moderate Monday Problem
*/

-- Key Insights:
-- • Most schools have "Friday Problems" - attendance drops on Fridays
-- • Some schools have "Monday Problems" - slow starts to the week
-- • Wednesday typically has highest attendance (mid-week stability)

-- ========================================
-- 3. ROLLING TREND ANALYSIS SAMPLE RESULTS
-- ========================================

-- Expected Output:
/*
school          | grade_level | latest_date | current_7day_trend | current_30day_baseline | trend_direction | weekly_change | alert_level | trend_status | volatility_level
Jefferson High  | 9          | 2024-03-15  | 73.2              | 85.1                  | -11.9          | -8.3         | CRITICAL    | DECLINING    | MODERATE VOLATILITY
Lincoln High    | 10         | 2024-03-15  | 84.1              | 86.7                  | -2.6           | -1.2         | WARNING     | STABLE       | LOW VOLATILITY
Washington High | 11         | 2024-03-15  | 91.3              | 89.8                  | 1.5            | 2.1          | HEALTHY     | IMPROVING    | LOW VOLATILITY
*/

-- Key Insights:
-- • Jefferson High Grade 9 needs immediate attention (73.2% vs 85.1% baseline)
-- • Lincoln High Grade 10 is stable but under warning threshold
-- • Washington High Grade 11 is improving and healthy

-- ========================================
-- 4. COHORT ANALYSIS SAMPLE RESULTS
-- ========================================

-- Expected Output:
/*
school          | grade_level | enrollment_cohort | age_cohort     | cohort_size | cohort_attendance_rate | consistency_score | cohort_status      | consistency_level
Lincoln High    | 9          | Fall Enrollees    | Typical Age    | 45          | 92.3                  | 18.4             | TOP PERFORMING     | Very Consistent
Lincoln High    | 9          | Spring Enrollees  | Younger Students| 12          | 78.1                  | 31.2             | NEEDS ATTENTION    | Moderately Consistent  
Washington High | 10         | Fall Enrollees    | Typical Age    | 38          | 89.7                  | 22.1             | AVERAGE            | Very Consistent
Washington High | 10         | Winter Enrollees  | Older Students | 8           | 94.2                  | 15.3             | TOP PERFORMING     | Very Consistent
*/

-- Key Insights:
-- • Fall enrollees generally perform better (established routines)
-- • Younger students may need additional support
-- • Smaller cohorts can sometimes outperform due to individual attention

-- ========================================
-- 5. AUTOMATED ALERT SYSTEM SAMPLE RESULTS
-- ========================================

-- Expected Output:
/*
priority_level          | notification_level      | school        | grade_level | current_rate | trend | weekly_change | alert_message
URGENT: Declining Critical | Principal + Superintendent | Jefferson High| 9          | 73.2        | -11.9 | -8.3         | ALERT: Jefferson High Grade 9 has 73.2% attendance (DECLINING)
HIGH: Volatile Warning     | Principal + Counselor      | Lincoln High  | 12         | 83.1        | -1.2  | 12.4         | ALERT: Lincoln High Grade 12 has 83.1% attendance (STABLE/IMPROVING)
MEDIUM: Declining Watch    | Teacher + Counselor        | Washington High| 11         | 87.8        | -3.1  | -2.1         | ALERT: Washington High Grade 11 has 87.8% attendance (DECLINING)
*/

-- Key Insights:
-- • Immediate intervention needed for Jefferson High Grade 9
-- • Lincoln High Grade 12 is volatile but stable - monitor closely
-- • Washington High Grade 11 shows early warning signs

-- ========================================
-- IMPLEMENTATION STEPS FOR YOUR ENVIRONMENT
-- ========================================

-- Step 1: Database Setup and Indexing
-- Run these commands first for optimal performance:

-- Essential indexes
CREATE INDEX idx_attendance_date_student ON attendance_events(date, student_id) USING BTREE;
CREATE INDEX idx_attendance_student_date ON attendance_events(student_id, date) USING BTREE;
CREATE INDEX idx_students_school_grade ON all_students(hometown, grade_level) USING BTREE;

-- Step 2: Create the summary table for dashboards
CREATE TABLE daily_attendance_summary (
    summary_date DATE,
    school VARCHAR(100),
    grade_level INT,
    present_count INT,
    total_count INT,
    attendance_rate DECIMAL(5,2),
    day_of_week TINYINT,
    month_num TINYINT,
    year_num SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_date, school, grade_level),
    INDEX idx_summary_date (summary_date),
    INDEX idx_summary_school_grade (school, grade_level)
);

-- Step 3: Population script for summary table (run daily)
INSERT INTO daily_attendance_summary 
(summary_date, school, grade_level, present_count, total_count, attendance_rate, day_of_week, month_num, year_num)
SELECT 
    ae.date,
    s.hometown,
    s.grade_level,
    COUNT(CASE WHEN ae.attendance = 1 THEN 1 END),
    COUNT(*),
    ROUND(COUNT(CASE WHEN ae.attendance = 1 THEN 1 END) * 100.0 / COUNT(*), 2),
    DAYOFWEEK(ae.date),
    MONTH(ae.date),
    YEAR(ae.date)
FROM all_students s
JOIN attendance_events ae ON s.student_id = ae.student_id
WHERE ae.date = CURDATE() - INTERVAL 1 DAY  -- Yesterday's data
GROUP BY ae.date, s.hometown, s.grade_level
ON DUPLICATE KEY UPDATE 
    present_count = VALUES(present_count),
    total_count = VALUES(total_count),
    attendance_rate = VALUES(attendance_rate);

-- Step 4: Create views for common queries
CREATE VIEW v_current_week_attendance AS
SELECT 
    school,
    grade_level,
    AVG(attendance_rate) as week_avg,
    STDDEV(attendance_rate) as week_volatility,
    COUNT(*) as days_this_week
FROM daily_attendance_summary
WHERE summary_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
AND day_of_week BETWEEN 2 AND 6
GROUP BY school, grade_level;

CREATE VIEW v_monthly_trends AS
SELECT 
    school,
    grade_level,
    year_num,
    month_num,
    AVG(attendance_rate) as monthly_avg,
    STDDEV(attendance_rate) as monthly_volatility,
    COUNT(*) as school_days
FROM daily_attendance_summary
WHERE summary_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
GROUP BY school, grade_level, year_num, month_num;

-- ========================================
-- DASHBOARD QUERIES (FAST EXECUTION)
-- ========================================

-- Quick Dashboard Query 1: Current Status
SELECT 
    school,
    grade_level,
    ROUND(week_avg, 2) as current_week_rate,
    ROUND(week_volatility, 2) as volatility,
    days_this_week,
    CASE 
        WHEN week_avg < 75 THEN 'CRITICAL'
        WHEN week_avg < 85 THEN 'WARNING'
        WHEN week_avg < 90 THEN 'WATCH'
        ELSE 'HEALTHY'
    END as status
FROM v_current_week_attendance
ORDER BY week_avg ASC;

-- Quick Dashboard Query 2: Monthly Comparison
SELECT