# Week 4: Data Structures - Dictionaries and Sets

## Learning Objectives
- Work with dictionaries for key-value storage
- Use sets for unique collections
- Understand hash-based data structures
- Apply appropriate data structures to problems

## Topics Covered

### 1. Dictionaries
- Creating and accessing dictionaries
- Dictionary methods (get, keys, values, items)
- Dictionary comprehensions
- Nested dictionaries

### 2. Sets
- Creating and using sets
- Set operations (union, intersection, difference)
- Frozen sets
- Set comprehensions

### 3. Choosing Data Structures
- When to use each structure
- Performance considerations
- Real-world applications
- Data structure conversions

## Practice Exercises

```python
# 1. Dictionary operations
user = {"name": "John", "age": 30, "city": "NYC"}
user["email"] = "john@example.com"
for key, value in user.items():
    print(f"{key}: {value}")

# 2. Set operations
set_a = {1, 2, 3, 4}
set_b = {3, 4, 5, 6}
print(set_a.union(set_b))
print(set_a.intersection(set_b))

# 3. Dictionary comprehension
squares = {x: x**2 for x in range(1, 6)}
```

## Resources
- Python dictionary documentation
- Set operations guide
- Data structure comparison

## Status
⏳ **PENDING**

## Prerequisites
- Week 3: Data Structures - Lists and Tuples
