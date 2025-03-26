# Core Functionality Tests

## Array Initialization
Successful initialization with valid parameters
Handling of invalid sizes (zero or negative)
Memory allocation correctness
Type handling for different numeric types (i32, i64, f32, f64)

## Numeric Array from String
Parsing of integers (both positive and negative)
Parsing of floating-point values
Handling of empty values in the input string
Handling of whitespace in the input
Handling of completely empty strings
Correct setting of validity bits

## String Array from String
Correct parsing of strings
Proper handling of empty strings within the input
Correct offset calculation
Proper validity bit handling
Memory management for the string data


# Edge Cases and Error Handling

## Invalid Inputs
Malformed input strings (e.g., "1, 2, abc" for numeric arrays)
Very large input values (testing for overflow)
Empty or null inputs


## Resource Management
Memory allocation for large arrays
Proper cleanup when operations fail


# Utility Functions

## Print Functions
Correct output formatting
Handling of null/invalid values
Boundary cases (empty arrays, arrays with mixed valid/invalid values)


# Integration Tests

## End-to-End Usage
Creation, modification, and printing of arrays in sequence
Using arrays with other components of your system


# Performance Tests (Optional)

## Performance Benchmarks
Processing time for large arrays
Memory usage patterns


# Specific Test Cases for Your Functions

## initialise_numeric_array
Test with different numeric types
Test with different sizes
Test allocator behavior

## numeric_array_from_string
Test with integers, floating point
Test with valid and invalid values mixed
Test overflow handling

## string_array_from_string
Test with various string patterns
Test with empty strings and null values
Test with very long strings

## print_numeric_array and print_string_array
Test output correctness
Test behavior with null values