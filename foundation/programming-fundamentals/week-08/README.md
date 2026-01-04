# Week 8: Introduction to Data Processing with Python

## Learning Objectives
- Work with NumPy for numerical computing
- Introduction to Pandas for data manipulation
- Basic data cleaning and transformation
- Apply Python to data engineering tasks

## Topics Covered

### 1. NumPy Basics
- Arrays and array operations
- Array indexing and slicing
- Mathematical operations
- Broadcasting

### 2. Pandas Introduction
- Series and DataFrames
- Reading and writing data
- Basic data exploration
- Selecting and filtering data

### 3. Data Processing
- Handling missing data
- Data type conversions
- Grouping and aggregation
- Sorting and merging

## Practice Exercises

```python
# 1. NumPy operations
import numpy as np
arr = np.array([1, 2, 3, 4, 5])
print(arr.mean(), arr.std())

# 2. Pandas DataFrame
import pandas as pd
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000, 60000, 70000]
})
print(df.describe())

# 3. Data filtering
high_earners = df[df['salary'] > 55000]
print(high_earners)
```

## Resources
- NumPy documentation
- Pandas documentation
- Data processing tutorials
- Kaggle datasets for practice

## Status
⏳ **PENDING**

## Prerequisites
- Week 7: Object-Oriented Programming Basics

## Course Completion
After completing this week, you will have finished the Programming Fundamentals course!
