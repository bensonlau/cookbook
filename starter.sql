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
*/

select 
	s.user_id,
	round(avg(if(c.action="confirmed",1,0)),2) as confirmation_rate
from Signups as s
left join Confirmations as c 
	on s.user_id= c.user_id
group by user_id;

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
