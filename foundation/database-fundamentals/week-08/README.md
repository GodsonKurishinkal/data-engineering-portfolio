# Week 8: NoSQL and Modern Database Concepts

## Learning Objectives
- Understand NoSQL database types
- Work with document databases
- Compare SQL vs. NoSQL
- Apply appropriate database solutions

## Topics Covered

### 1. NoSQL Database Types
- Document stores (MongoDB)
- Key-value stores (Redis)
- Column-family stores (Cassandra)
- Graph databases (Neo4j)

### 2. Document Databases
- MongoDB basics
- Collections and documents
- CRUD operations in MongoDB
- Querying and indexing

### 3. Database Selection
- CAP theorem
- SQL vs. NoSQL trade-offs
- Polyglot persistence
- Database scalability patterns

## Practice Exercises

```javascript
// MongoDB examples

// 1. Insert document
db.users.insertOne({
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    skills: ["Python", "SQL", "MongoDB"]
});

// 2. Query documents
db.users.find({ age: { $gte: 25 } });

// 3. Update document
db.users.updateOne(
    { email: "john@example.com" },
    { $set: { age: 31 } }
);
```

## SQL vs NoSQL Comparison

| Feature | SQL | NoSQL |
|---------|-----|-------|
| Schema | Fixed | Flexible |
| Scalability | Vertical | Horizontal |
| Transactions | ACID | Eventually Consistent |
| Best for | Complex queries | High throughput |

## Resources
- MongoDB University
- NoSQL database comparison
- CAP theorem explained
- Database architecture patterns

## Status
⏳ **PENDING**

## Prerequisites
- Week 7: Advanced SQL and Performance

## Course Completion
After completing this week, you will have finished the Database Fundamentals course!
