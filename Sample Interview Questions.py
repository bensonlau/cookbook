https://leetcode.com/problems/department-top-three-salaries/


Table: Employee

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| id           | int     |
| name         | varchar |
| salary       | int     |
| departmentId | int     |
+--------------+---------+
id is the primary key (column with unique values) for this table.
departmentId is a foreign key (reference column) of the ID from the Department table.
Each row of this table indicates the ID, name, and salary of an employee. It also contains the ID of their department.


Table: Department

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
+-------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table indicates the ID of a department and its name.


A company's executives are interested in seeing who earns the most money in each of the company's departments.

Write a solution to find the employees who are high earners in each of the departments.
 A high earner in a department is an employee who has a salary in the top three unique salaries for that department.

-------

If we think that we can predict digital sales in an area based on the sales made by stores in an area,
how might you approach estimating the impact.  How would you begin select variables?

Q3. Differentiate between univariate, bivariate, and multivariate analysis.

What cross-validation technique would you use on a time series data set?


----
Q6. You are given a data set consisting of variables with more than 30 percent missing values. How will you deal with them?
If the data set is large, we can just simply remove the rows with missing data values. It is the quickest way; we use the rest of the data to predict the values.
For smaller data sets, we can impute missing values with the mean, median, or average of the rest of the data using pandas data frame in python. There are different ways to do so, such as:
df.mean(), df.fillna(mean)


Q22. During analysis, how do you treat missing values?
The extent of the missing values is identified after identifying the variables with missing values. If any patterns are identified the analyst has to concentrate on them as it could lead to interesting and meaningful business insights.
If there are no patterns identified, then the missing values can be substituted with mean or median values (imputation) or they can simply be ignored. Assigning a default value which can be mean, minimum or maximum value. Getting into the data is important.
If it is a categorical variable, the default value is assigned. The missing value is assigned a default value. If you have a distribution of data coming, for normal distribution give the mean value.
If 80% of the values for a variable are missing, then you can answer that you would be dropping the variable instead of treating the missing values.