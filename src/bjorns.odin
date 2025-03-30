package bjorns

import "base:intrinsics"
import vmem "core:mem/virtual"
import "core:math/rand"
import "core:strings"
import "core:strconv"
import "core:fmt"
import "core:encoding/csv"
import "core:os"
import "core:text/table"
import "core:math"

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

Column :: struct {
    name: string,
    category: typeid,
    data: union {
        // TODO(will): Create array types for all data categories
        NumericArray(i8),
        NumericArray(i16),
        NumericArray(i32),
        NumericArray(i64),
        NumericArray(u8),
        NumericArray(u16),
        NumericArray(u32),
        NumericArray(u64),
        NumericArray(f32),
        NumericArray(f64),
        NumericArray(int),
        NumericArray(uint),
        StringArray,
    },
    dropped: bool
}

DataFrame :: struct {
    columns: map[string]Column,
    column_order: [dynamic]string,
    shape: [2]int,
    is_mutable: bool,
}

ColumnReadInfo :: struct {
    category: typeid,
    bytesize: int,
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
    values := strings.split(data, ",", context.temp_allocator)
    defer(free_all(context.temp_allocator))

    if len(values) == 0 {
        return {}, false
    }

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
    values := strings.split(data, ",", context.temp_allocator)
    defer(free_all(context.temp_allocator))

    string_count := len(values)
    if string_count == 0 {
        return {}, false
    }

    offset_length := string_count-1
    strings_combined := strings.concatenate(values, context.temp_allocator)

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
                    ((array.validity[byte_index] >> bit_position & 1) == 1)
                    
        if is_valid {
            fmt.printfln("%s", transmute(string)array.values[start:end])
        } else {
            fmt.println("null")
        }
    }
}

init_dataframe :: proc(allocator := context.allocator) -> DataFrame {
    df := DataFrame{
        columns = make(map[string]Column, 0, allocator),
        column_order = make([dynamic]string, allocator),
        shape = {0, 0},
        is_mutable = true,
    }
    return df
}

detect_type :: proc(s: string) -> string {
    // Handle empty string, "NA", or "NULL" as null
    if len(s) == 0 || strings.to_upper(s) == "NA" || strings.to_upper(s) == "NULL" {
        return "null"
    }
    
    // Try parsing as int first
    if _, ok := strconv.parse_int(s); ok {
        return "int"
    }
    
    // If not int, try as float
    if f, ok := strconv.parse_f64(s); ok {
        // Check if f32 is sufficient (no precision loss)
        if f == f64(f32(f)) {
            return "f32"
        }
        return "f64"
    }
    
    // If neither int nor float, it's a string
    return "string"
}

get_column_info :: proc(rows: [][]string, index: int) -> ColumnReadInfo {
    string_count := 0
    f64_count := 0
    f32_count := 0
    int_count := 0

    byte_count := 0
    for row, i in rows {
        value := row[index]
        value_type := detect_type(value)

        byte_count += len(value)
        switch value_type {
            case "string": string_count += 1
            case "f64": f64_count += 1
            case "f32": f32_count += 1
            case "int": int_count += 1
        }

        if (i >= 1000) && string_count == 0 {
            break
        }
    }
    
    column_category: typeid
    if string_count > 0 {
        column_category = string
    } else if f64_count > 0 {
        column_category = f64
    } else if f32_count > 0 {
        column_category = f32
    } else {
        column_category = int
    }

    return(
        ColumnReadInfo{
            category = column_category,
            bytesize = byte_count,
        }
    )
}

read_numeric_data :: proc(array: ^$A/NumericArray($T), rows: [][]string, col_idx: int) {
    for row, i in rows {
        value := row[col_idx]
        byte_index := u64(i) / 8
        bit_position := u64(i) % 8

        switch array.a_type {
            case u8, u16, u32, u64, uint:
                parsed_value, ok := strconv.parse_uint(value)
                array.values[i] = ok ? T(parsed_value) : 0
                if ok {
                    array.validity[byte_index] |= 1 << bit_position
                }
            case i8, i16, i32, i64, int:
                parsed_value, ok := strconv.parse_int(value)
                array.values[i] = ok ? T(parsed_value) : 0
                if ok {
                    array.validity[byte_index] |= 1 << bit_position
                }
            case f32:
                parsed_value, ok := strconv.parse_f32(value)
                array.values[i] = ok ? T(parsed_value) : 0
                if ok {
                    array.validity[byte_index] |= 1 << bit_position
                }
            case f64:
                parsed_value, ok := strconv.parse_f64(value)
                array.values[i] = ok ? T(parsed_value) : 0
                if ok {
                    array.validity[byte_index] |= 1 << bit_position
                }
        }
    }
}

add_numeric_column :: proc(df: ^DataFrame, name: string, col_idx: int, $T: typeid, rows: [][]string, allocator := context.allocator) {
    array, ok := initialise_numeric_array(T, len(rows), allocator) // TODO(will): handle if this fails
    read_numeric_data(&array, rows, col_idx)
    df.columns[name] = Column {
        name = name,
        category = T,
        data = array,
        dropped = false
    }
}

read_string_data :: proc(array: ^StringArray, col_idx: int, rows: [][]string) {
    n_rows := len(rows)
    byte_count := 0
    for row, i in rows {
        value := row[col_idx]
        byte_index := u64(i) / 8
        bit_position := u64(i) % 8

        if value != "" && value != "NA" && value != "NULL" {
            array.validity[byte_index] |= 1 << bit_position
        }
        if i < n_rows-1 {
            byte_count += len(value)
            array.offsets[i] = i32(byte_count)
        }

        start := i == 0 ? 0 : array.offsets[i-1]
        end := i >= len(array.offsets) ? i32(len(array.values)) : i32(byte_count)
        copy(array.values[start:end], transmute([]byte)value)
    }
}

read_csv :: proc(filename: string, allocator := context.allocator) -> (DataFrame, bool) {
    df := init_dataframe(allocator)
    
    r: csv.Reader
	r.trim_leading_space  = true
	r.reuse_record        = true // Without it you have to delete(record)
	r.reuse_record_buffer = true // Without it you have to each of the fields within it
	defer csv.reader_destroy(&r)

	csv_data, ok := os.read_entire_file(filename)
	if ok {
		csv.reader_init_with_string(&r, string(csv_data))
	} else {
		fmt.printfln("Unable to open file: %v", filename)
		return {}, false
	}
	defer delete(csv_data)

	records, err := csv.read_all(&r)
	if err != nil { /* Do something with CSV parse error */ }

	defer {
		for rec in records {
			delete(rec)
		}
		delete(records)
	}

    n_rows := len(records)-1
    for column_name, i in records[0] {
        column_read_info := get_column_info(records[1:], i)

        switch column_read_info.category {
            case i8: add_numeric_column(&df, column_name, i, i8, records[1:], allocator)
            case i16: add_numeric_column(&df, column_name, i, i16, records[1:], allocator)
            case i32: add_numeric_column(&df, column_name, i, i32, records[1:], allocator)
            case i64: add_numeric_column(&df, column_name, i, i64, records[1:], allocator)
            case u8: add_numeric_column(&df, column_name, i, u8, records[1:], allocator)
            case u16: add_numeric_column(&df, column_name, i, u16, records[1:], allocator)
            case u32: add_numeric_column(&df, column_name, i, u32, records[1:], allocator)
            case u64: add_numeric_column(&df, column_name, i, u64, records[1:], allocator)
            case f32: add_numeric_column(&df, column_name, i, f32, records[1:], allocator)
            case f64: add_numeric_column(&df, column_name, i, f64, records[1:], allocator)
            case string:
                offset_length := n_rows-1
                new_array := StringArray {
                    values = make([]byte, column_read_info.bytesize, allocator),
                    validity = make([]byte, (n_rows + 7) / 8, allocator),
                    offsets = make([]i32, offset_length, allocator),
                    a_type = string,
                    length = column_read_info.bytesize,
                    is_mutable = false,
                }
                read_string_data(&new_array, i, records[1:])
                df.columns[column_name] = Column {
                    name = column_name,
                    category = string,
                    data = new_array,
                    dropped = false
                }
        }

        append(&df.column_order, column_name)
    }
    
    df.shape = {n_rows, len(df.column_order)}
    return df, true
}

handle_numeric_array :: proc(tbl: ^table.Table, row_idx, col_idx: int, 
                            values: $T, validity: []byte, byte_index, bit_position: u64) {
    is_valid := byte_index < u64(len(validity)) && 
                ((validity[byte_index] >> bit_position & 1) == 1)
                
    if !is_valid {
        table.set_cell_value(tbl, row_idx, col_idx, "null")
    } else {
        value := values[row_idx]
        table.set_cell_value(tbl, row_idx, col_idx, value)
    }
}

print_dataframe :: proc(df: DataFrame, max_rows: int = 10) {
    stdout := table.stdio_writer()
    tbl: table.Table
	table.init(&tbl)
    defer table.destroy(&tbl)

    table.padding(&tbl, 1, 1) // Left/right padding of cells

    for column in df.column_order {
        table.header(&tbl, column)
    }

    total_rows := math.min(max_rows, df.shape[0])
    for row_idx in 0..<total_rows {
        table.row(&tbl)
        for column, col_idx in df.column_order {
            byte_index := u64(row_idx) / 8
            bit_position := u64(row_idx) % 8

            column_data := df.columns[column].data

            switch variant in column_data {
                case NumericArray(int):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(i8):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(i16):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(i32):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(i64):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(uint):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(u8):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(u16):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(u32):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(u64):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(f32):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case NumericArray(f64):
                    handle_numeric_array(&tbl, row_idx, col_idx, variant.values, variant.validity, byte_index, bit_position)
                case StringArray:
                    data_array := variant.values[:]
                    validity_array := variant.validity[:]
                    offsets_array := variant.offsets[:]
                    
                    is_valid := byte_index < u64(len(validity_array)) && 
                        ((validity_array[byte_index] >> bit_position & 1) == 1)
                    
                    if !is_valid {
                        table.set_cell_value(&tbl, row_idx, col_idx, "null")
                    } else {
                        start := row_idx == 0 ? 0 : offsets_array[row_idx-1]
                        end := row_idx >= len(offsets_array) ? i32(len(data_array)) : offsets_array[row_idx]
                        value := transmute(string)data_array[start:end]
                        table.set_cell_value(&tbl, row_idx, col_idx, value)
                    }
            }
        }
    }
    
    table.write_plain_table(stdout, &tbl)
}

main :: proc() {
    df, ok := read_csv("src/customers-100.csv")
    if ok {
        print_dataframe(df)
    } else {
        fmt.println("failed")
    }
}