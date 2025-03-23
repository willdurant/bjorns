package bjorns

import "base:intrinsics"
import vmem "core:mem/virtual"
import "core:math/rand"
import "core:strings"
import "core:strconv"
import "core:fmt"

IntArray :: struct($T: typeid) where intrinsics.type_is_integer(T) {
    values: []T,
    validity: []byte,
    isize: typeid,
    length: int,
    is_mutable: bool,
}

initialise_int_array :: proc($T: typeid, N: int, allocator := context.allocator) -> (IntArray(T), bool) {
    if N <= 0 || !intrinsics.type_is_integer(T) {
        return {}, false
    }
    
    array := IntArray(T) {
        values = make([]T, N, allocator),
        validity = make([]byte, N, allocator),
        isize = T,
        length = N,
        is_mutable = false,
    }
    return array, true
}

int_array_from_string :: proc($T: typeid, data: string, allocator := context.allocator) -> (IntArray(T), bool) {
    values := strings.split(data, ",")

    if len(values) == 0 {
        return {}, false
    }

    new_array, ok := initialise_int_array(T, len(values), allocator)

    if !ok {
        return {}, false
    }

    for value, i in values {
        trimmed := strings.trim_space(value)
        n, ok := strconv.parse_int(trimmed)
        new_array.values[i] = ok ? T(n) : 0
        new_array.validity[i] = ok ? 1 : 0
    }

    return new_array, true
}

print_int_array :: proc(array: $A/IntArray($T)) where intrinsics.type_is_integer(T) {
    for i := 0; i < array.length; i += 1 {
        if array.validity[i] == 1 {
            fmt.printfln("%d", array.values[i])
        } else {
            fmt.println("null")
        }
    }
}

main :: proc() {
    level_arena: vmem.Arena
    arena_allocator := vmem.arena_allocator(&level_arena)

    test_string := "1, 456, 983, 22, 34, , 193, 7364, 13,,"
    test_array, ok := int_array_from_string(i64, test_string, arena_allocator)
    if ok {
        print_int_array(test_array)
    } else {
        fmt.println("fail")
    }
}