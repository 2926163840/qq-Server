#pragma once
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "Conn.hpp"

class QQThread;

class QQInfo
{
public:
    // 发消息时按 hash(convid) % threads.size() 找固定线程。
    std::vector<QQThread *> threads;
    // 所有连接（含未登入）key 是 Conn 裸指针。
    // Conn 在 accepter 创建时还没登入，不在 onlineuser 里；
    // 没人持有它的 shared_ptr 的话 accepter 返回后它就析构了 conns是主持有者
    std::unordered_map<Conn *, std::shared_ptr<Conn>> conns;
    // 记录在线用户
    std::unordered_map<long long, std::shared_ptr<Conn>> onlineuser;

    // 在线读写锁
    std::shared_mutex online_mutex;
    // 记录会话关联者的id
    std::unordered_map<long long, std::unordered_set<long long>> conv_touid;
    // 记录用户id关联的会话id
    std::unordered_map<long long, std::unordered_set<long long>> uid_toconnv;
    // 会话关联锁
    std::shared_mutex conv_mutex;
};