# Week 7: Object-Oriented Programming Basics

## Learning Objectives
- Understand OOP concepts
- Create and use classes
- Work with objects and methods
- Apply inheritance and encapsulation

## Topics Covered

### 1. Classes and Objects
- Class definition
- __init__ constructor
- Instance variables and methods
- self parameter

### 2. OOP Principles
- Encapsulation
- Inheritance
- Polymorphism (introduction)
- Class vs. instance attributes

### 3. Special Methods
- __str__ and __repr__
- __len__, __add__, etc.
- Property decorators
- Static and class methods

## Practice Exercises

```python
# 1. Basic class
class DataPipeline:
    def __init__(self, name, source):
        self.name = name
        self.source = source
        self.is_running = False
    
    def start(self):
        self.is_running = True
        print(f"{self.name} pipeline started")
    
    def stop(self):
        self.is_running = False
        print(f"{self.name} pipeline stopped")

# 2. Inheritance
class ETLPipeline(DataPipeline):
    def __init__(self, name, source, destination):
        super().__init__(name, source)
        self.destination = destination
    
    def transform(self, data):
        return data.upper()
```

## Resources
- OOP in Python tutorial
- Class design best practices
- Real-world OOP examples

## Status
⏳ **PENDING**

## Prerequisites
- Week 6: File I/O and Error Handling
