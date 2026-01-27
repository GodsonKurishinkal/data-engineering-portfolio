# Week 3: Data Structures - Lists and Tuples

## Learning Objectives
- Work with lists and tuples
- Perform common operations on sequences
- Understand mutability vs. immutability
- Apply data structures to real problems

## Topics Covered

### 1. Lists
- Creating and accessing lists
- List methods (append, extend, insert, remove)
- List slicing
- List comprehensions

### 2. Tuples
- Creating and using tuples
- Tuple packing and unpacking
- When to use tuples vs. lists
- Named tuples

### 3. Common Operations
- Sorting and searching
- List/tuple conversions
- Nested sequences
- Copying lists

## Practice Exercises

```python
# 1. List operations
numbers = [1, 2, 3, 4, 5]
numbers.append(6)
numbers.extend([7, 8])
squares = [x**2 for x in numbers]

# 2. Tuple operations
coordinates = (10, 20)
x, y = coordinates  # unpacking

# 3. Nested structures
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
print(matrix[1][1])  # Access element
```

## Resources
- Python list documentation
- Data structure tutorials
- Practice problems

## Status
⏳ **PENDING**

## Prerequisites
- Week 2: Control Flow - Conditionals and Loops
