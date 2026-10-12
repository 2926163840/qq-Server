#pragma once

#include <memory>
#include <unordered_map>
#include <array>
#include <mutex>

#define SEQ_MUTEX_NUM 512

// 序列分配
class SeqAlloctor
{
private:
    // 关联id，关联seq
    std::unordered_map<long long, long long> alloc;
    // 分片锁
    std::array<std::mutex, SEQ_MUTEX_NUM> seqlock;

public:
    // 查找关联id的下一个序列
    long long getSeq(long long id)
    {
        // 算出id哈希值获取分片锁下标
        int idx = std::hash<long long>{}(id) % SEQ_MUTEX_NUM;
        // 加锁
        std::lock_guard<std::mutex> lock(seqlock.at(idx));
        auto it = alloc.find(id);
        //
        if (it == alloc.end())
        {
            return -1;
        }
        return ++it->second;
    }
    // 懒加载需要用的时候发现不活跃在初始化
    long long initSeq(long long id, long long seq)
    {

        // 算出id哈希值获取分片锁下标
        int idx = std::hash<long long>{}(id) % SEQ_MUTEX_NUM;
        // 加锁
        std::lock_guard<std::mutex> lock(seqlock.at(idx));
        // first放插入成功的迭代器失败返回已经存在的key的迭代器 second是是否插入成功
        auto it = alloc.emplace(id, seq).first;
        return ++it->second;
    }
};