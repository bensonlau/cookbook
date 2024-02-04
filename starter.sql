/* 
SQL Stuff
@bensonlau

 */


/*Counting Column Length
-- MYSQL
-- Usecase: Identifying columns of certain length of entire column or of the string within column
1. LENGTH() returns the length of the string measured in bytes.
2. CHAR_LENGTH() returns the length of the string measured in characters.

--Reference:
1. https://www.w3schools.com/sql/func_mysql_character_length.asp
2. https://www.w3schools.com/sql/func_mysql_length.asp
*/


SELECT 
	field_id
FROM table
WHERE CHAR_LENGTH(content) > 15


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
--Usecase: Identifying percent of users with consecutive days of activity using their earliest day of activity

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
--Find percentage of first orders that the requested delivery is on the same date as the order
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
round(100*sum(immediate) / count(distinct c.customer_id),2) as immediate_percentage
from customer_first_order_summary c

