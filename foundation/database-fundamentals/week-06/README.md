# Week 6: Database Design and Normalization

## Learning Objectives
- Design effective database schemas
- Understand normalization forms
- Apply normalization principles
- Balance normalization with performance

## Topics Covered

### 1. Database Design Principles
- Requirements analysis
- Conceptual design (ER diagrams)
- Logical design
- Physical design

### 2. Normalization
- First Normal Form (1NF)
- Second Normal Form (2NF)
- Third Normal Form (3NF)
- Boyce-Codd Normal Form (BCNF)

### 3. Design Considerations
- Denormalization trade-offs
- Indexing strategy
- Data integrity constraints
- Schema evolution

## Practice Exercises

```sql
-- 1. Creating tables with constraints
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    manager_id INT,
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- 2. Adding constraints
ALTER TABLE employees
ADD CONSTRAINT chk_salary CHECK (salary >= 0);

-- 3. Indexes
CREATE INDEX idx_employee_lastname 
ON employees(last_name);
```

## Resources
- Database design patterns
- Normalization tutorials
- ER diagram tools
- Design best practices

## Status
⏳ **PENDING**

## Prerequisites
- Week 5: Data Manipulation - INSERT, UPDATE, DELETE
