# Week 7: Advanced SQL and Performance

## Learning Objectives
- Write complex SQL queries
- Optimize query performance
- Use window functions
- Work with CTEs and views

## Topics Covered

### 1. Advanced SQL Features
- Common Table Expressions (CTEs)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- Views and materialized views
- Stored procedures basics

### 2. Query Optimization
- Execution plans
- Index optimization
- Query rewriting
- Statistics and cardinality

### 3. Advanced Techniques
- PIVOT and UNPIVOT
- Recursive queries
- Array and JSON functions
- Full-text search

## Practice Exercises

```sql
-- 1. Window function
SELECT 
    first_name, 
    last_name, 
    salary,
    RANK() OVER (ORDER BY salary DESC) as salary_rank
FROM employees;

-- 2. CTE example
WITH dept_avg AS (
    SELECT 
        department_id,
        AVG(salary) as avg_salary
    FROM employees
    GROUP BY department_id
)
SELECT e.*, d.avg_salary
FROM employees e
JOIN dept_avg d ON e.department_id = d.department_id;

-- 3. Create view
CREATE VIEW high_earners AS
SELECT * FROM employees
WHERE salary > 75000;
```

## Resources
- Window functions guide
- Query optimization techniques
- Execution plan analysis
- Advanced SQL tutorials

## Status
⏳ **PENDING**

## Prerequisites
- Week 6: Database Design and Normalization
