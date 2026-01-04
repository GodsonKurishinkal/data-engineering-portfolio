# Week 5: Functions and Modules

## Learning Objectives
- Define and call functions
- Understand function parameters and return values
- Work with modules and packages
- Apply modular programming principles

## Topics Covered

### 1. Functions
- Function definition and syntax
- Parameters and arguments
- Return values
- Default parameters and keyword arguments
- *args and **kwargs

### 2. Scope and Lifetime
- Local vs. global scope
- Nonlocal variables
- Variable lifetime
- Best practices for scope

### 3. Modules
- Importing modules
- Creating custom modules
- __name__ == "__main__"
- Python Standard Library overview

## Practice Exercises

```python
# 1. Function definition
def calculate_total(price, tax_rate=0.08):
    """Calculate total price including tax."""
    return price * (1 + tax_rate)

total = calculate_total(100)

# 2. Multiple return values
def get_user_info():
    return "John", 30, "Engineer"

name, age, job = get_user_info()

# 3. Module usage
import math
import random

result = math.sqrt(16)
random_num = random.randint(1, 100)
```

## Resources
- Python functions documentation
- Module and package tutorial
- Standard Library reference

## Status
⏳ **PENDING**

## Prerequisites
- Week 4: Data Structures - Dictionaries and Sets
