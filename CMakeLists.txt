cmake_minimum_required(VERSION 3.16)
project(server)

#头文件路径
#include_directories(&{PROJECT_SOURCE_DIR}/include)

#启用gdb
add_compile_options(-g)

#c++版本
set(CMAKE_CXX_STANDARD 20)
#使用标准纯C++20
set(CMAKE_CXX_EXTENSIONS OFF)
#加载pkgconfig模块
find_package(PkgConfig REQUIRED)
#找libevent库
pkg_check_modules(EVENT REQUIRED libevent libevent_pthreads)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)
pkg_check_modules(MYSQL REQUIRED mysqlclient)
#设置源文件
set(SRC 
    main.cc
    include/Conn.hpp
    include/FastStmt.hpp
    include/Log.hpp
    include/QQDatabase.hpp
    include/QQInfo.hpp
    include/QQServer.hpp
    include/QQThread.hpp
    include/QQThreadPool.hpp
    include/SeqAlloctor.hpp
    )

# 搜索源文件
# aux_source_directory(&{PROJECT_SOURCE_DIR}/源文件目录a A)
# 搜索源文件
# aux_source_directory(&{PROJECT_SOURCE_DIR}/源文件目录b B)
# 合并源文件
# set(SRC A B)

#搜索源文件 GLOB不递归 GLOB_RECURSE递归
# file(GLOB SRC &{PROJECT_SOURCE_DIR}/*.cc)

#生成的可执行文件名称和源文件资源
add_executable(server ${SRC})
#头文件搜索路径
target_include_directories(server PRIVATE
    include/
    ${EVENT_INCLUDE_DIRS}
    ${JSONCPP_INCLUDE_DIRS}
    ${MYSQL_INCLUDE_DIRS}
)
#库文件搜索路径

target_link_directories(server PRIVATE
    ${EVENT_LIBRARY_DIRS}
    ${JSONCPP_LIBRARY_DIRS}
    ${MYSQL_LIBRARY_DIRS}

)
#链接库
target_link_libraries(server PRIVATE
    event
    event_pthreads
    jsoncpp
    mysqlclient
)


#设置动静态库的输出路径 动态库 CMAKE_LIBRARY_OUTPUT_DIRECTORY 
#                      可执行文件 CMAKE_RUNTIME_OUTPUT_DIRECTORY 
#                      静态库 CMAKE_ARCHIVE_OUTPUT_DIRECTORY
#set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY 路径)
#添加动静态库
#add_library(库名 SHARED/STATIC ${SRC})
#连接静态库
#link_libraries(库名)
#链接静态库路径
#link_directories(路径)
#链接动态库 默认PUBLIC
#target_link_libraries(目标文件 <PUBLIC/PRIVATE/INTERFACE>链接库名1 <PUBLIC/PRIVATE/INTERFACE>链接库名2...)
#PUBLIC多层链接  PRIVATE 单次链接 有符号 INTERFACE 无法链接仅有符号可调用（自然无法知道库函数是谁的） 

#打印日志
#message(字符串或${变量})
#设置字符串
#set(str 字符串)
#拼接字符串
#set(strs ${str1} ${str2}...)
#list(APPEND str ${str1} ${str2}...)
#删除字符串            绝对路径  也可以#${PROJECT_SOURCE_DIR}/相对路径/文件名
#list(REMOVE_ITEM str ${str1} ${str2}...)
#获取长度
#list(LENGTH SRC len)

#给当前节点添加子目录 默认相对路径
#add_subdirectory(路径)
