# Week 4: SQL Joins and Subqueries

## Learning Objectives
- Understand different types of joins
- Write effective join queries
- Use subqueries for complex queries
- Combine data from multiple tables

## Topics Covered

### 1. Types of Joins
- INNER JOIN
- LEFT JOIN (LEFT OUTER JOIN)
- RIGHT JOIN (RIGHT OUTER JOIN)
- FULL OUTER JOIN
- CROSS JOIN

### 2. Join Conditions
- ON clause
- USING clause
- Multiple join conditions
- Self joins

### 3. Subqueries
- Subqueries in WHERE clause
- Subqueries in SELECT clause
- Correlated subqueries
- EXISTS and IN operators

## Practice Exercises

```sql
-- 1. Inner join
SELECT e.first_name, e.last_name, d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;

-- 2. Left join
SELECT e.first_name, e.last_name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id;

-- 3. Subquery
SELECT first_name, last_name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);
```

## Resources
- SQL joins visual guide
- Subquery examples
- Join performance optimization

## Status
⏳ **PENDING**

## Prerequisites
- Week 3: SQL - Aggregate Functions and Grouping
