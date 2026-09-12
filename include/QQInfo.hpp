#pragma once
#include <unordered_map>
#include<unordered_set>
#include <memory>
#include <mutex>


class QQInfo
{
public:
    // 记录在线用户
    std::unordered_map<long long, struct bufferevent *> onlineuser;
    // 在线用户反查找
    std::unordered_map<struct bufferevent *, long long> bev_toid;
    // 在线锁
    std::mutex online_mutex;
    //记录会话关联者的id
    std::unordered_map<long long,std::unordered_set<long long>>conv_touid;
    //记录用户id关联的会话id
    std::unordered_map<long long,std::unordered_set<long long>>uid_toconnv;
};