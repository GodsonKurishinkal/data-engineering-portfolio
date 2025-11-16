# Week 6: File I/O and Error Handling

## Learning Objectives
- Read from and write to files
- Handle exceptions properly
- Work with different file formats
- Implement robust error handling

## Topics Covered

### 1. File Operations
- Opening and closing files
- Reading files (read, readline, readlines)
- Writing to files
- Context managers (with statement)

### 2. Exception Handling
- try-except blocks
- Multiple except clauses
- else and finally
- Raising exceptions
- Custom exceptions

### 3. Working with Data Files
- CSV files
- JSON files
- Text processing
- File paths and os module

## Practice Exercises

```python
# 1. File reading
with open('data.txt', 'r') as file:
    content = file.read()
    print(content)

# 2. Exception handling
try:
    result = 10 / 0
except ZeroDivisionError:
    print("Cannot divide by zero!")
finally:
    print("Cleanup code here")

# 3. JSON handling
import json

data = {"name": "John", "age": 30}
with open('data.json', 'w') as f:
    json.dump(data, f)
```

## Resources
- File I/O documentation
- Exception handling guide
- Working with CSV and JSON

## Status
⏳ **PENDING**

## Prerequisites
- Week 5: Functions and Modules
