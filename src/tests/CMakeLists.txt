cmake_minimum_required(VERSION 3.1)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/)
#file(GLOB testFiles "*.cpp")
set(testFiles
    compile_test.cpp
    manage_db.cpp
    storage.cpp
    buffer.cpp
    index.cpp
    index_unique_1.cpp
    aggregation.cpp
    row.cpp
    schema.cpp
)

link_libraries(
    ${STANDARD_LIBRARIES}
)

foreach (testFile ${testFiles})
    string(REGEX MATCH "([^\/]+$)" filename ${testFile})
    string(REGEX MATCH "[^.]*" executable_name tests_${filename})
    add_executable(${executable_name} ${testFile})
    target_link_libraries(${executable_name} ${STANDARD_LIBRARIES} )
    add_test(NAME "tests/${filename}" COMMAND ${executable_name})
endforeach ()
