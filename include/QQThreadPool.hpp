#pragma once
#include"QQDatabase.hpp"
#include"QQThread.hpp"
#include"QQInfo.hpp"
#include<vector>
#include <memory>

class QQThreadpool{

private:
    // struct QQThreadDeleter{
    //     void operator()(QQThread*temp)
    //     {
    //         event_base_loopbreak(temp->getBase());
    //     }
    // };

    std::unique_ptr<QQInfo>qqinfo;
    //线程容器
    std::vector<std::unique_ptr<QQThread>>vthread;
    int threadnum;
    int index=0;

public:
    QQThreadpool(QQDatabase*qqdb,int num)
    {
        qqinfo=std::make_unique<QQInfo>();
        threadnum=num;
        for(int i=0;i<threadnum;i++)
        {
            vthread.emplace_back(std::make_unique<QQThread>(qqdb,qqinfo.get(),i));
        }
    }
    ~QQThreadpool()
    {
    }

    QQThread* getThread()
    {
        index++;
        int next=index%threadnum;
        return vthread[next].get();
    }
};