package bjorns_test

import "core:testing"
import "core:fmt"
import "core:mem"
import "core:strings"
import bjorns "../src"

@(test)
test_initialise_numeric_array :: proc(t: ^testing.T) {
    // Test with i32
    {
        array, ok := bjorns.initialise_numeric_array(i32, 5)
        testing.expect(t, ok, "Expected successful initialization")
        testing.expect_value(t, array.length, 5)
        testing.expect_value(t, len(array.values), 5)
        testing.expect_value(t, len(array.validity), 1)
        testing.expect_value(t, array.a_type, i32)
        
        // Cleanup
        delete(array.values)
        delete(array.validity)
    }

    // Test with uint
    {
        array, ok := bjorns.initialise_numeric_array(uint, 22)
        testing.expect(t, ok, "Expected successful initialization")
        testing.expect_value(t, array.length, 22)
        testing.expect_value(t, len(array.values), 22)
        testing.expect_value(t, len(array.validity), 3)
        testing.expect_value(t, array.a_type, uint)
        
        // Cleanup
        delete(array.values)
        delete(array.validity)
    }
    
    // Test with f64
    {
        array, ok := bjorns.initialise_numeric_array(f64, 10)
        testing.expect(t, ok, "Expected successful initialization for f64")
        testing.expect_value(t, array.length, 10)
        testing.expect_value(t, len(array.values), 10)
        testing.expect_value(t, len(array.validity), 2)
        testing.expect_value(t, array.a_type, f64)
        
        // Cleanup
        delete(array.values)
        delete(array.validity)
    }
    
    // Test invalid size
    {
        array, ok := bjorns.initialise_numeric_array(i32, 0)
        testing.expect(t, !ok, "Expected failure with size 0")
        
        array, ok = bjorns.initialise_numeric_array(i32, -5)
        testing.expect(t, !ok, "Expected failure with negative size")
    }
}

main :: proc() {
    // Setup test context with reporting
    t := testing.T{}
    
    // Run the tests
    test_initialise_numeric_array(&t)

}