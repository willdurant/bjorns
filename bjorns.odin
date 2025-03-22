package bjorns

import "base:intrinsics"
import vmem "core:mem/virtual"
import "core:math/rand"

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

main :: proc() {
    level_arena: vmem.Arena
    arena_allocator := vmem.arena_allocator(&level_arena)
    int32_array, ok := initialise_int_array(i32, rand.int_max(300), arena_allocator)
}