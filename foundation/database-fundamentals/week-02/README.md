# Week 2: SQL Basics - Data Retrieval

## Learning Objectives
- Write SELECT queries
- Filter and sort data
- Use basic SQL functions
- Understand query execution

## Topics Covered

### 1. SELECT Statement
- Basic SELECT syntax
- Selecting specific columns
- Using aliases
- DISTINCT keyword

### 2. Filtering Data
- WHERE clause
- Comparison operators
- Logical operators (AND, OR, NOT)
- BETWEEN, IN, LIKE operators

### 3. Sorting and Limiting
- ORDER BY clause
- ASC and DESC
- LIMIT and OFFSET
- NULL handling

## Practice Exercises

```sql
-- 1. Basic select
SELECT first_name, last_name, salary
FROM employees
WHERE salary > 50000
ORDER BY salary DESC;

-- 2. Using LIKE for pattern matching
SELECT * FROM employees
WHERE last_name LIKE 'S%';

-- 3. Multiple conditions
SELECT * FROM employees
WHERE salary BETWEEN 40000 AND 70000
AND hire_date >= '2020-01-01'
ORDER BY hire_date;
```

## Resources
- SQL SELECT tutorial
- Query practice platforms
- SQL fiddle online

## Status
⏳ **PENDING**

## Prerequisites
- Week 1: Introduction to Databases and SQL
