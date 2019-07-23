cmake_minimum_required(VERSION 2.8) 
add_compile_options(-std=c++17 -fPIC)
file(GLOB docdblib_SRC "*.cpp" "linux/*.cpp")
file(GLOB docdblib_HDR "*.h" "*.tcc" "linux/*.h" "linux/*.tcc")
add_library (docdblib ${docdblib_SRC})
# target_include_directories (docdblib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS docdblib
        DESTINATION "lib"
        ) 
INSTALL(FILES ${docdblib_HDR}
        DESTINATION "include/docdblib" 
        )              