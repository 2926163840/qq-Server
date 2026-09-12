#include "./include/QQServer.hpp"
#include <iostream>
#include <event2/thread.h>
int main()
{
    // 开启安装全局pthread锁使bufferevent变为线程安全的
    evthread_use_pthreads();
    QQServer server;
    server.listen(9999);
    return 0;
}