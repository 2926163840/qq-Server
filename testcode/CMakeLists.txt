cmake_minimum_required(VERSION 3.16)
project(testcode)
#c++版本
set(CMAKE_CXX_STANDARD 20)
#标准版本
set(CMAKE_CXX_EXTENSIONS OFF)
#开启PkgConvif模块
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
#搜索json库文件
pkg_check_modules(Jsoncpp REQUIRED jsoncpp)

#取出所有测试文件
file(GLOB SRC ./*.cpp)
#遍历生成可执行文件
foreach(src ${SRC})
    get_filename_component(name ${src} NAME_WE)
    add_executable(${name} ${src})
    target_include_directories(${name} PRIVATE ${Jsoncpp_INCLUDE_DIRS})
    target_link_directories(${name} PRIVATE ${Jsoncpp_LIBRARY_DIRS})
    target_link_libraries(${name} PRIVATE ${Jsoncpp_LIBRARIES} Threads::Threads)
endforeach()
