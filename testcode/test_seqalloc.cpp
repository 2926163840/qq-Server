#include "../include/SeqAlloctor.hpp"
#include <cassert>
#include <thread>
#include <vector>
#include <set>
#include <mutex>
#include <iostream>

int main()
{
    // 1. 未初始化的 id，getSeq 返回 -1
    SeqAlloctor sac;
    assert(sac.getSeq(100) == -1);

    // 2. initSeq(100, 0) 返回 1（第一个序列是 1）
    assert(sac.initSeq(100, 0) == 1);

    // 3. 连续 getSeq 严格递增
    assert(sac.getSeq(100) == 2);
    assert(sac.getSeq(100) == 3);
    assert(sac.getSeq(100) == 4);

    // 4. 不同 id 互不影响
    assert(sac.getSeq(200) == -1);
    assert(sac.initSeq(200, 0) == 1);
    assert(sac.getSeq(200) == 2);
    assert(sac.getSeq(100) == 5);

    // 5. 大 id 不越界（验证取模修复）
    assert(sac.initSeq(1000000001LL, 0) == 1);
    assert(sac.getSeq(1000000001LL) == 2);

    // 6. 并发分配 8000 次不重复、不丢号
    SeqAlloctor sac2;
    sac2.initSeq(1, 0);
    std::vector<std::thread> threads;
    std::set<long long> results;
    std::mutex mtx;
    for (int i = 0; i < 8; i++)
    {
        threads.emplace_back([&]
                             {
            for (int j = 0; j < 1000; j++)
            {
                long long s = sac2.getSeq(1);
                std::lock_guard<std::mutex> lk(mtx);
                results.insert(s);
            } });
    }
    for (auto &t : threads)
        t.join();
    assert(results.size() == 8000);
    assert(*results.begin() == 2);
    assert(*results.rbegin() == 8001);

    std::cout << "SeqAllocator 所有测试通过\n";
    return 0;
}