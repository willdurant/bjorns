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

StringArray :: struct {
    values: []byte,
    validity: []byte,
    offsets: []i32,
    a_type: typeid,
    length: int,
    is_mutable: bool,
}

initialise_numeric_array :: proc($T: typeid, N: int, allocator := context.allocator, string_count: i32 = 0) -> (NumericArray(T), bool) {
    if N <= 0 {
        return {}, false
    }
    
    if intrinsics.type_is_integer(T) || intrinsics.type_is_float(T) {
        array := NumericArray(T) {
            values = make([]T, N, allocator),
            validity = make([]byte, (N + 7) / 8, allocator),
            a_type = T,
            length = N,
            is_mutable = false,
        }

        return array, true
    }

    return {}, false
}


numeric_array_from_string :: proc($T: typeid, data: string, allocator := context.allocator) -> (NumericArray(T), bool) {
    values := strings.split(data, ",") // TODO(will): Change to temporary allocator?

    if len(values) == 0 {
        return {}, false
    }

    // TODO(will): need to implement string array reading
    // TODO(will): Make this DRY
    if intrinsics.type_is_integer(T) {
        new_array, ok := initialise_numeric_array(T, len(values), allocator)
        if !ok {
            return {}, false
        }
        for value, i in values {
            // TODO(will): need to manage potential int overflow
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_int(trimmed)
            new_array.values[i] = ok ? T(n) : 0

            byte_index := u64(i) / 8
            bit_position := u64(i) % 8
            if ok {
                new_array.validity[byte_index] |= 1 << bit_position
            }
        }

        return new_array, true
    }

    if T == f32 {
        new_array, ok := initialise_numeric_array(T, len(values), allocator)
        if !ok {
            return {}, false
        }
        for value, i in values {
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_f32(trimmed)
            new_array.values[i] = ok ? T(n) : 0
            
            byte_index := u64(i) / 8
            bit_position := u64(i) % 8
            if ok {
                new_array.validity[byte_index] |= 1 << bit_position
            }
        }

        return new_array, true
    }

    if T == f64 {
        new_array, ok := initialise_numeric_array(T, len(values), allocator)
        if !ok {
            return {}, false
        }
        for value, i in values {
            trimmed := strings.trim_space(value)
            n, ok := strconv.parse_f64(trimmed)
            new_array.values[i] = ok ? T(n) : 0
            
            byte_index := u64(i) / 8
            bit_position := u64(i) % 8
            if ok {
                new_array.validity[byte_index] |= 1 << bit_position
            }
        }

        return new_array, true
    }

    return {}, false
}

string_array_from_string :: proc(data: string, allocator := context.allocator) -> (StringArray, bool) {
    values := strings.split(data, ",") // TODO(will): Change to temporary allocator?
    string_count := len(values)
    offset_length := string_count-1
    strings_combined := strings.concatenate(values) // TODO(will): Change to temporary allocator?

    new_array := StringArray {
        values = make([]byte, len(strings_combined), allocator),
        validity = make([]byte, (string_count + 7) / 8, allocator),
        offsets = make([]i32, offset_length, allocator),
        a_type = string,
        length = len(strings_combined),
        is_mutable = false,
    }
    copy(new_array.values, transmute([]byte)strings_combined)

    byte_count := 0
    for value, i in values {
        if i < string_count-1 {
            byte_count += len(value)
            new_array.offsets[i] = i32(byte_count)
        }

        byte_index := u64(i) / 8
        bit_position := u64(i) % 8
        if value != "" {
            new_array.validity[byte_index] |= 1 << bit_position
        }
    }

    return new_array, true
}

print_numeric_array :: proc(array: $A/NumericArray($T)) {
    // TODO(will): implement string array printing
    if intrinsics.type_is_integer(T) {
        for i := 0; i < array.length; i += 1 {
            byte_index := u64(i) / 8
            bit_position := u64(i) % 8

            if ((array.validity[byte_index] >> bit_position) & 1) == 1 {
                fmt.printfln("%d", array.values[i])
            } else {
                fmt.println("null")
            }
        }
    }

    if intrinsics.type_is_float(T) {
        for i := 0; i < array.length; i += 1 {
            byte_index := u64(i) / 8
            bit_position := u64(i) % 8

            if ((array.validity[byte_index] >> bit_position) & 1) == 1 {
                fmt.printfln("%f", array.values[i])
            } else {
                fmt.println("null")
            }
        }
    }
}

print_string_array :: proc(array: StringArray) {
    // TODO(will): Fix preceding whitespace occuring in strings
    string_length := len(array.offsets) + 1
    for i := 0; i < string_length; i += 1 {
        start := i == 0 ? 0 : array.offsets[i-1]
        end := i >= len(array.offsets) ? i32(len(array.values)) : array.offsets[i]

        byte_index := u64(i) / 8
        bit_position := u64(i) % 8

        // Check if we're within bounds of the validity array
        is_valid := byte_index < u64(len(array.validity)) && 
                    ((array.validity[byte_index] >> bit_position) & 1) == 1
                    
        if is_valid {
            fmt.printfln("%s", transmute(string)array.values[start:end])
        } else {
            fmt.println("null")
        }
    }
}

main :: proc() {
    level_arena: vmem.Arena
    arena_allocator := vmem.arena_allocator(&level_arena)

    test_string := "1, 456, 983, 22, 34, , 193, 7364, 13,,"
    test_array, ok := numeric_array_from_string(i64, test_string, arena_allocator)
    if ok {
        fmt.println("integer array")
        print_numeric_array(test_array)
    } else {
        fmt.println("fail")
    }

    test2_string := "3.98, 2.578, 167.9811, 25.163, 79.0, , 145.2, 13333.2114, 0.097,,"
    test2_array, ok2 := numeric_array_from_string(f64, test2_string, arena_allocator)
    if ok2 {
        fmt.println("float array")
        print_numeric_array(test2_array)
    } else {
        fmt.println("fail")
    }

    test3_string := "banana, elephant, jdifhu4, , junk,, snsauipkm, lol, benahgdhks, hato,,"
    test3_array, ok3 := string_array_from_string(test3_string, arena_allocator)
    if ok3 {
        fmt.println("string array")
        print_string_array(test3_array)
    } else {
        fmt.println("fail")
    }
}