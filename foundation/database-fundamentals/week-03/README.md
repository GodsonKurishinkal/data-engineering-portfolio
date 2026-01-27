# Week 3: SQL - Aggregate Functions and Grouping

## Learning Objectives
- Use aggregate functions
- Group data with GROUP BY
- Filter groups with HAVING
- Analyze data with aggregations

## Topics Covered

### 1. Aggregate Functions
- COUNT, SUM, AVG
- MIN, MAX
- DISTINCT with aggregates
- Handling NULL values

### 2. GROUP BY Clause
- Grouping by single column
- Grouping by multiple columns
- Aggregating grouped data
- Understanding groups

### 3. HAVING Clause
- Filtering aggregated results
- HAVING vs. WHERE
- Complex conditions
- Performance considerations

## Practice Exercises

```sql
-- 1. Basic aggregation
SELECT 
    COUNT(*) as total_employees,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary
FROM employees;

-- 2. Grouping
SELECT 
    department_id,
    COUNT(*) as employee_count,
    AVG(salary) as avg_dept_salary
FROM employees
GROUP BY department_id;

-- 3. Using HAVING
SELECT 
    department_id,
    AVG(salary) as avg_salary
FROM employees
GROUP BY department_id
HAVING AVG(salary) > 60000;
```

## Resources
- Aggregate functions guide
- GROUP BY examples
- SQL aggregation practice

## Status
⏳ **PENDING**

## Prerequisites
- Week 2: SQL Basics - Data Retrieval
