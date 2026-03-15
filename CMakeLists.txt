cmake_minimum_required(VERSION 3.23)
project(b3sbe LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
execute_process(
    COMMAND ${Python_EXECUTABLE} -c "import pyarrow, pathlib, sys; print(pathlib.Path(pyarrow.__file__).resolve().parent)"
    OUTPUT_VARIABLE PYARROW_SITE
    OUTPUT_STRIP_TRAILING_WHITESPACE)

set(PYARROW_INCLUDE_DIR "${PYARROW_SITE}/include")
file(GLOB PYARROW_ARROW_LIB "${PYARROW_SITE}/libarrow.so*")
file(GLOB PYARROW_PARQUET_LIB "${PYARROW_SITE}/libparquet.so*")
list(LENGTH PYARROW_ARROW_LIB _arrow_len)
list(LENGTH PYARROW_PARQUET_LIB _parquet_len)
if(_arrow_len EQUAL 0 OR _parquet_len EQUAL 0)
    message(FATAL_ERROR "Unable to find libarrow/libparquet inside ${PYARROW_SITE}")
endif()
list(GET PYARROW_ARROW_LIB 0 PYARROW_ARROW_LIB_PATH)
list(GET PYARROW_PARQUET_LIB 0 PYARROW_PARQUET_LIB_PATH)

set(CPP_DECODER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cpp_decoder)

set(B3SBE_CORE_SOURCES
    ${CPP_DECODER_DIR}/pcap_reader.cpp
    ${CPP_DECODER_DIR}/decoder.cpp
    ${CPP_DECODER_DIR}/parquet_writer.cpp
)

add_library(b3sbe_core STATIC ${B3SBE_CORE_SOURCES})
target_include_directories(b3sbe_core
    PUBLIC
        ${CPP_DECODER_DIR}
        ${CPP_DECODER_DIR}/sbe-generated
        ${PYARROW_INCLUDE_DIR})
target_link_libraries(b3sbe_core
    PUBLIC
        ${PYARROW_PARQUET_LIB_PATH}
        ${PYARROW_ARROW_LIB_PATH})
set_target_properties(b3sbe_core PROPERTIES BUILD_RPATH ${PYARROW_SITE})

if (MSVC)
    target_compile_options(b3sbe_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(b3sbe_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(b3sbe_decode ${CPP_DECODER_DIR}/main.cpp)
target_link_libraries(b3sbe_decode PRIVATE b3sbe_core)
set_target_properties(b3sbe_decode PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
