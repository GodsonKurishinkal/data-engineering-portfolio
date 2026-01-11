# Week 1: Introduction to Databases and SQL

## Learning Objectives
- Understand database concepts and terminology
- Learn about relational databases
- Introduction to SQL
- Set up database environment

## Topics Covered

### 1. Database Fundamentals
- What is a database?
- DBMS vs. Database
- Relational vs. Non-relational databases
- ACID properties

### 2. Relational Database Concepts
- Tables, rows, and columns
- Primary and foreign keys
- Relationships (one-to-one, one-to-many, many-to-many)
- Entity-Relationship Diagrams

### 3. SQL Introduction
- SQL syntax basics
- Data types
- Database engines (PostgreSQL, MySQL, SQLite)
- Setting up a database

## Practice Exercises

```sql
-- 1. Create a simple table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    hire_date DATE,
    salary DECIMAL(10, 2)
);

-- 2. Verify table structure
DESCRIBE employees;
```

## Resources
- PostgreSQL documentation
- SQLite for practice
- DB design tutorials
- ER diagram tools

## Status
⏳ **PENDING**

## Prerequisites
- None (entry point for databases)
