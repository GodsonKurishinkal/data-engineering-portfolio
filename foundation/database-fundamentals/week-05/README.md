# Week 5: Data Manipulation - INSERT, UPDATE, DELETE

## Learning Objectives
- Insert data into tables
- Update existing records
- Delete records safely
- Understand transaction basics

## Topics Covered

### 1. INSERT Statement
- Single row insert
- Multiple row insert
- INSERT INTO SELECT
- Default values and auto-increment

### 2. UPDATE Statement
- Updating single records
- Updating multiple records
- Conditional updates
- UPDATE with joins

### 3. DELETE Statement
- Deleting specific records
- DELETE with conditions
- TRUNCATE vs. DELETE
- Cascading deletes

### 4. Transactions
- BEGIN, COMMIT, ROLLBACK
- Transaction isolation
- ACID properties in practice
- Savepoints

## Practice Exercises

```sql
-- 1. Insert data
INSERT INTO employees (first_name, last_name, hire_date, salary)
VALUES ('John', 'Doe', '2024-01-01', 55000);

-- 2. Update data
UPDATE employees
SET salary = salary * 1.10
WHERE department_id = 3;

-- 3. Delete data
DELETE FROM employees
WHERE hire_date < '2015-01-01';

-- 4. Transaction example
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

## Resources
- SQL DML documentation
- Transaction management guide
- Best practices for data manipulation

## Status
⏳ **PENDING**

## Prerequisites
- Week 4: SQL Joins and Subqueries
