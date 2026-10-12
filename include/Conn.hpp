#pragma once

#include <memory>
#include <atomic>
#include <mutex>
#include <deque>
#include <string>
#include <event2/event.h>
#include <event2/bufferevent.h>

// 前向声明
class QQThread;

struct Conn;

struct SendTask
{
    // 连接对象保活防止在排队过程中析构
    std::shared_ptr<Conn> conn;
    // json序列化数据
    std::string data;
    // 消息拥有者
    long long userid = -1;
    // 命令
    std::string cmd = "";
};

// 收发消息将会话投递到故定线程保证收发顺序
struct DBTask
{
    /// 入库失败时需要发送放的bev返回
    // std::shared_ptr<Conn> conn;

    long long convid;
    long long sendid;
    std::string content;
    std::string uuid;
};

// 连接对象  // 断连回调 eventCallBack 里只有裸指针 ctx，需要 shared_from_this()
//  拿回管理它的 shared_ptr 来保活，防止函数执行到一半对象被析构。
struct Conn : std::enable_shared_from_this<Conn>
{
    // 连接对象所属线程
    QQThread *thread = nullptr;
    // 连接对象所属base
    struct event_base *base = nullptr;
    // 连接对象的bufferevent
    struct bufferevent *bev = nullptr;
    // 连接存活标志
    std::atomic<bool> alive{true};
    // 连接用户id
    long long userid = -1;
    // 最后活跃时间(毫秒时间戳 心跳保活用 归属线程读写无需原子)
    long long last_active = 0;
    // 写任务唤醒时间
    struct event *notify_ev = nullptr;
    // 写任务队列 + 锁。
    // send_mtx 保护 send_queue、alive、notify_ev 的激活/摘除，
    // 让"投递侧 event_active"和"断连侧 event_free"互斥（操作同一 event 是 UB）。
    std::mutex send_mtx;
    std::deque<SendTask *> send_queue;
    // 兜底释放
    ~Conn()
    {
        // 这些任务持有 shared_ptr<Conn>，不删它们 Conn 引用不归零、永远析构不了（泄漏）。
        for (SendTask *t : send_queue)
            delete t;
        if (notify_ev)
            event_free(notify_ev);
    }
};
