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


SELECT field_id
FROM table
WHERE CHAR_LENGTH(content) > 15

