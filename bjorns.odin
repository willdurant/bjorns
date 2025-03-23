package bjorns

import "base:intrinsics"
import vmem "core:mem/virtual"
import "core:math/rand"
import "core:strings"
import "core:strconv"
import "core:fmt"

Type_Category :: enum {
    // Numeric types
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16, // Not native to Odin
    Float32,
    Float64,
    
    // Boolean type
    Bool,
    
    // String types
    String,
    LargeString,
    
    // Binary types
    Binary,
    LargeBinary,
    
    // Date/Time types
    Date32,  // Days since epoch
    Date64,  // Milliseconds since epoch
    Time32,  // Seconds/Milliseconds
    Time64,  // Microseconds/Nanoseconds
    Timestamp,
    
    // Complex types
    Decimal,
    List,
    Map,
    Struct,
    
    // Default/fallback
    Null,
}

NumericArray :: struct($T: typeid) where intrinsics.type_is_integer(T) || intrinsics.type_is_float(T) {
    values: []T,
    validity: []byte,
    a_type: typeid,
    length: int,
    is_mutable: bool,
}

initialise_array :: proc($T: typeid, N: int, allocator := context.allocator) -> (NumericArray(T), bool) {
    if N <= 0 {
        return {}, false
    }
    
    if intrinsics.type_is_integer(T) || intrinsics.type_is_float(T) {
        array := NumericArray(T) {
            values = make([]T, N, allocator),
            validity = make([]byte, N, allocator),
            a_type = T,
            length = N,
            is_mutable = false,
        }

        return array, true
    }

    return {}, false
}

array_from_string :: proc($T: typeid, data: string, allocator := context.allocator) -> (NumericArray(T), bool) {
    values := strings.split(data, ",")

    if len(values) == 0 {
        return {}, false
    }

    new_array, ok := initialise_array(T, len(values), allocator)

    if !ok {
        return {}, false
    }

    if intrinsics.type_is_integer(T) {
        for value, i in values {
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_int(trimmed)
            new_array.values[i] = ok ? T(n) : 0
            new_array.validity[i] = ok ? 1 : 0
        }

        return new_array, true
    }

    if T == f32 {
        for value, i in values {
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_f32(trimmed)
            new_array.values[i] = ok ? T(n) : 0
            new_array.validity[i] = ok ? 1 : 0
        }

        return new_array, true
    }

    if T == f64 {
        for value, i in values {
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_f64(trimmed)
            new_array.values[i] = ok ? T(n) : 0
            new_array.validity[i] = ok ? 1 : 0
        }

        return new_array, true
    } 

    return {}, false
}



print_array :: proc(array: $A/NumericArray($T)) {
    if intrinsics.type_is_integer(T) {
        for i := 0; i < array.length; i += 1 {
            if array.validity[i] == 1 {
                fmt.printfln("%d", array.values[i])
            } else {
                fmt.println("null")
            }
        }
    }

    if intrinsics.type_is_float(T) {
        for i := 0; i < array.length; i += 1 {
            if array.validity[i] == 1 {
                fmt.printfln("%f", array.values[i])
            } else {
                fmt.println("null")
            }
        }
    }
}

main :: proc() {
    level_arena: vmem.Arena
    arena_allocator := vmem.arena_allocator(&level_arena)

    test_string := "1, 456, 983, 22, 34, , 193, 7364, 13,,"
    test_array, ok := array_from_string(i64, test_string, arena_allocator)
    if ok {
        fmt.println("integer array")
        print_array(test_array)
    } else {
        fmt.println("fail")
    }

    test2_string := "3.98, 2.578, 167.9811, 25.163, 79.0, , 145.2, 13333.2114, 0.097,,"
    test2_array, ok2 := array_from_string(f64, test2_string, arena_allocator)
    if ok2 {
        fmt.println("float array")
        print_array(test2_array)
    } else {
        fmt.println("fail")
    }
}