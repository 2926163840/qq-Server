#pragma once

#include <iostream>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/util.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>

#include "Log.hpp"
#include "QQThreadPool.hpp"
#include "QQDatabase.hpp"
#include "Conn.hpp"

#define THREAD_NUM 30

class QQServer
{
private:
    struct event_base *m_base;
    struct evconnlistener *m_listener;
    struct bufferevent *m_bev;
    std::unique_ptr<QQThreadpool> pool;
    std::unique_ptr<QQDatabase> qqdb;

public:
    QQServer()
    {
        // 创建一个事件基础对象
        m_base = event_base_new();
        // 创建数据库对象
        qqdb = std::make_unique<QQDatabase>(THREAD_NUM);
        // 创建线程池对象
        pool = std::make_unique<QQThreadpool>(qqdb.get(), THREAD_NUM);
    };
    ~QQServer()
    {
        // 释放evconnlistener事件内存
        if (m_listener)
            evconnlistener_free(m_listener);
        // 释放事件集合内存
        if (m_base)
            event_base_free(m_base);
    };

    void listen(int port)
    {
        // 设置监听sockaddr_in
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        int addrlen = sizeof(addr);
        // expoll监听绑定
        // 绑定失败
        m_listener = evconnlistener_new_bind(m_base, accepter, this, LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE, 1024, (sockaddr *)&addr, addrlen);
        if (!m_listener)
        {
            lg()(Fatal, SCREEN, "QQServer evconnlistener_new_bind failed: {}", strerror(errno));
            return;
        }
        // 死循环阻塞持续监听
        event_base_dispatch(m_base);
    }
    // 接收函数
    static void accepter(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *addr, int socklen, void *ptr)
    {
        // 获取this
        QQServer *server = static_cast<QQServer *>(ptr);
        // 获取相应线程(顺序获取)
        QQThread *thread = server->pool->getThread();
        // 连接对象
        std::shared_ptr<Conn> conn = std::make_shared<Conn>();
        // 连接对象所属线程
        conn->thread = thread;
        // 连接对象所属base
        conn->base = thread->getBase();
        // 初始化最后活跃时间(心跳保活)
        conn->last_active = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        // 创建socket事件
        conn->bev = bufferevent_socket_new(thread->getBase(), fd, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
        // 创建失败关闭套接字
        if (conn->bev == nullptr)
        {

            lg()(Error, SCREEN, "QQServer acceptr bufferevent_socket_new failed{}", fd);
            evutil_closesocket(fd);
            return;
        }
        // 成功提示信息
        else
            lg()(Info, SCREEN, "new client connected");

        // 写任务唤醒事件
        conn->notify_ev = event_new(conn->base, -1, EV_PERSIST, QQThread::drain_cb, conn.get());
        event_add(conn->notify_ev, NULL);
        // 放入连接哈希表中保活
        {
            QQInfo *info = server->pool->getInfo();
            std::lock_guard<std::shared_mutex> lock(info->online_mutex);
            info->conns[conn.get()] = conn;
        }
        // 设置回调函数
        bufferevent_setcb(conn->bev, QQThread::readCallBack, QQThread::writeCallBack, QQThread::eventCallBack, conn.get());
        // 开启读事件监听
        bufferevent_enable(conn->bev, EV_READ);
    }
};