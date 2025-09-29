/* 
SQL Stuff
@bensonlau

 */


/*Counting Column Length
-- MYSQL
-- Usecase: Identifying columns of certain length of entire column or of the 
--string within column
1. LENGTH() returns the length of the string measured in bytes.
2. CHAR_LENGTH() returns the length of the string measured in characters.

--Reference:
1. https://www.w3schools.com/sql/func_mysql_character_length.asp
2. https://www.w3schools.com/sql/func_mysql_length.asp
*/


select 
	field_id
from table
where char_length(content) > 15


/*Updating columns
--MYSQL
--Usecase: fix the names so that only the first character is 
uppercase and the rest are lowercase.

--Reference:
1. https://www.w3schools.com/sql/func_mysql_substr.asp
*/

select
  u.user_id,
  concat(upper(substr(u.name,1,1)),
      lower(substr(u.name,2))) AS name 
from Users u
order by
  u.user_id asc

/*Summarizing a dataset*/
--MYSQL
--Usecase: find all the classes with at least x number of students
--Approach #1: Using Group By and the Having condition

select
    class
from Courses
group by class
having count(distinct student) >= 5

--Approach #2: Using nested subquery
--CTE can also be used for readability, reusability

--References:
--https://learnsql.com/blog/cte-vs-subquery/

select
    class
from
    (select
        class, count(student) AS num
    FROM
        courses
    group by class) AS temp
where
    num >= 5

/*Calculating Ratio or Weighted Metrics
---MYSQL
---Usecase: Deriving relative frequency of an action

--Reference:
1. https://www.w3schools.com/sql/func_mysql_ifnull.asp
*/

select 
	s.user_id,
	round(avg(if(c.action="confirmed",1,0)),2) as confirmation_rate
from Signups as s
left join Confirmations as c 
	on s.user_id= c.user_id
group by user_id;

select 
	s.user_id, 
	ifnull(round(sum(c.action='confirmed')/count(*), 2), 0) as confirmation_rate 
from Signups as s 
left join Confirmations as c 
	on s.user_id = c.user_id 
group by s.user_id


/*Calculating Volume Metrics Conditionally
--MYSQL
--Usecase: Deriving absolute measurement of an action

--Reference:
1. https://www.w3schools.com/sql/func_mysql_substring.asp
*/

select
    substring(trans_date,1,7) as month, 
    country,
    count(distinct id) as trans_count,
    count(distinct if(state = 'approved',id,null)) as approved_count,
    sum(amount) as trans_total_amount,
    sum(if(state='approved',amount,0)) as approved_total_amount

from Transactions T
group by 1,2

/*Calculating Ratio or Weighted Metric Conditionally Using Self-Joins
--MYSQL
--Usecase: Identifying percent of users with consecutive days of activity using 
--their earliest day of activity

--Reference:
1. https://www.w3schools.com/sql/func_mysql_date_sub.asp
*/

with earliest_login_summary as (
  select
    player_id,
    min(event_date) as first_login
  from
    Activity
  group by
    player_id
)

, consec_logins as (
  select
    a.player_id as player_id
  from
    earliest_login_summary e
    inner join activity a 
        on e.player_id = a.player_id
            and e.first_login = DATE_SUB(a.event_date, interval 1 DAY)
)

select
  round (
    (select count(distinct c.player_id) from consec_logins c)
    / (select count(e.player_id) from earliest_login_summary e)
  , 2) as fraction;

/*Calculating Ratio/Percentages Using Subqueries*/
--MYSQL
--Usecase: Find percentage of first orders that the requested delivery 
--is on the same date as the order
with sub as (
    select
        customer_id,
        min(order_date) as first_order_date
    from Delivery
    group by customer_id
)

, customer_first_order_summary as (
select
sub.customer_id,
case when D.customer_pref_delivery_date > sub.first_order_date then 0
    when D.customer_pref_delivery_date <= sub.first_order_date then 1
    end as immediate    
from sub
left join Delivery d
    on sub.customer_id = D.customer_id
        and sub.first_order_date = D.order_date
)

select
round(100*sum(immediate) 
  / count(distinct c.customer_id),2) as immediate_percentage
from customer_first_order_summary c

/*Creating running calculation to identify points in data*/
with cte as (
  select 
    person_name,
    weight,
    turn,
    sum(weight) OVER (order by turn) AS running_total_weight 
  from Queue
)

select
  person_name 
from cte
where running_total_weight <= 1000
order by turn desc
limit 1

/*Calculating top performers by categories*/
--MYSQl
--Usecase: 
--Write a solution to find the employees who are high earners in each of the 
--departments. A high earner in a department is an employee who has a salary in
-- the top three unique salaries for that department.
--Reference(s):
--1. https://www.sqlshack.com/overview-of-sql-rank-functions/
with sub as (

select
  e.id,
  e.name as Employee,
  e.salary,
  e.departmentId,
  d.name as Department,
  dense_rank() over 
    (partition by e.departmentId order by e.salary desc) as ranking
from Employee e
left join Department d
    on e.departmentId = d.id
)

select
  sub.Department as Department,
  sub.Employee as Employee,
  sub.salary as Salary
from sub
where ranking in ('1','2','3')
order by sub.salary asc

/*Identifying consecutively sequenced rows
--MYSQL
--Usecase: Identifying all numbers that appear at least three times 
--consecutively.
--1. https://www.geeksforgeeks.org/mysql-lead-and-lag-function/


*/

select distinct num as ConsecutiveNums
from

(
select 
  num,
  lead(num, 1) over (order by id) AS ld,
  lag(num, 1) over (order by id) AS lg
from logs
) t

where num=ld and num=lg;


/*Calculating multiple day moving averages
--MYSQL
--Usecase: For every date that there is 7 days worth of data finding 
out how much was paid in that 7-day window (i.e., current day + 6 days
before). Should be rounded to two decimal places.
--Reference(s):
--1. https://stevestedman.com/2013/04/rows-and-range-preceding-and-following/
*/

select 
  visited_on,
  amount, 
  round(amount/7, 2) as average_amount
from (
    select distinct visited_on, 
    sum(amount) OVER(order by visited_on RANGE BETWEEN INTERVAL 6 DAY
      PRECEDING AND CURRENT ROW) as amount, 
    MIN(visited_on) over() as 1st_date 
    from Customer
    ) t
where visited_on>= 1st_date+6;

/*Identifying earliest entry
--MYSQL
--Usecase: Write a solution to select the product id, year, quantity, and price 
--for the first year of every product sold.
--Reference(s):
--1. https://dev.mysql.com/doc/refman/8.0/en/correlated-subqueries.html
*/
select 
    product_id, 
    year as first_year, 
    quantity, 
    price 
from Sales 
where 
(product_id, year) in (
    select 
        product_id, 
        min(year) AS year 
    from Sales 
    group by product_id
    );

/*Identifying records from dataset as it relates to another dataset
--MYSQL
--Usecase: Find the IDs of the users who visited without making any 
transactions and the number of times they made these types of visits.
--Reference(s):
*/

--Approach #1: Removing Records Using NOT IN/EXISTS
select 
    customer_id,
    count(transaction_id is null) as count_no_trans 
from visits
left join transactions 
  on visits.visit_id = transactions.visit_id
where transaction_id is null
group by customer_id;

--Approach #2: Removing Records left join and is null
select
    customer_id,
    count(distinct visit_id) as count_no_trans
from Visits v
where v.visit_id not in (
        select visit_id
        from Transactions
    )
group by customer_id;


/*Identifying records from dataset as it relates to another dataset
--MYSQL
--Usecase: Find all dates with higher temperature compared to previous
dates
*/

--Approach #1: Self join option
select
    w1.id
from Weather w1
left join Weather w2
    on datdiff(w1.recordDate, w2.recordDate) = 1 #w1 the day before, w2 the current day
where w2.temperature < w1.temperature


--Approach #2: Using window function option within a CTE
with PreviousWeatherData AS
(
    select 
      id,
      recordDate,
      temperature, 
      lag(temperature, 1) over (order by recordDate) AS PreviousTemperature,
      lag(recordDate, 1) over (order by recordDate) AS PreviousRecordDate
    from 
        Weather
)

select 
    id 
from 
    PreviousWeatherData
where 
  temperature > PreviousTemperature
    and 
  recordDate = date_add(PreviousRecordDate, interval 1 DAY);

/*Calculating difference between column values for two different rows*/
--MYSQL
--Usecase: 
/*
There is a factory website that has several machines each running the same number of processes.
Write a solution to find the average time each machine takes to complete a process.

The time to complete a process is the 'end' timestamp minus the 'start' timestamp. The average 
time is calculated by the total time to complete every process on the machine divided by the 
number of processes that were run.
The resulting table should have the machine_id along with the average time as processing_time, 
which should be rounded to 3 decimal places.
*/

--Approach #1: Self join and calculate from two columns from same row 
select a.machine_id, 
       round(avg(b.timestamp - a.timestamp), 3) as processing_time
from activity a, 
     activity b
where 
    a.machine_id = b.machine_id
and 
    a.process_id = b.process_id
and 
    a.activity_type = 'start'
and 
    b.activity_type = 'end'
group by machine_id

--Approach #2: Use case when and grouping functions
select 
    machine_id,
    round(
        SUM(case when activity_type='start' then timestamp*-1 else timestamp end)*1.0
    /(select count(distinct process_id)),3
        ) AS processing_time
from 
    activity
group by machine_id