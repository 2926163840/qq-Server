#pragma once

#include <format>
#include <iostream>
#include <string>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>

#define PATH "./Log/"

#define FILENUM 5

#define Debug 10001
#define Info 10002
#define Warning 10003
#define Error 10004
#define Fatal 10005

#define SCREEN 10006
#define FILE 10007

struct LogInfo
{
    std::string print;
    int method;
    int level;
};

class Log
{
private:
    // 条件变量
    std::condition_variable cv;
    std::mutex lg_tex;
    // 日至队列
    std::queue<LogInfo> logque;
    // std::shared_ptr<std::thread>s_thread;
    std::thread t;
    int fds[FILENUM] = {-1, -1, -1, -1, -1};

public:
    bool run = true;

public: // 初始化fd    开启任务函数
    Log()
    {
        initFile();
        t = std::thread(&Log::worker, this);
    }
    ~Log()
    {
        run = false;
        // 唤醒锁
        cv.notify_all();
        // 等待日志全部打印完毕
        if (t.joinable())
            t.join();
        // 关闭文件描述符
        closeFd();
    }
    // 重载（）接收信息
    template <typename... Arg>
    void operator()(int lv, int method, std::string_view fmt, Arg &&...arg)
    {
        // s_thread=std::make_shared<std::thread>(worker);
        // 拼接日志信息    format只接受字面量不接受字符串要用format 再使用makeformatargs将参数包变成formatargs不需要forward因为他不接受右值
        std::string info = std::vformat(fmt, std::make_format_args(arg...));
        // 获取当前时间
        time_t now = time(nullptr);
        struct tm time;
        // 线程安全版本
        localtime_r(&now, &time);
        // 获取日至等级
        std::string level = getLevel(lv);
        // 拼接完整信息
        std::stringstream ss;
        ss << std::this_thread::get_id();
        std::string print = std::format("[{}][{}][{:04}-{:02}-{:02} {:02}:{:02}:{:02}]:{}",
                                        level, ss.str(), time.tm_year + 1900, time.tm_mon + 1, time.tm_mday, time.tm_hour, time.tm_min, time.tm_sec, info);
        LogInfo li = {std::move(print), method, lv};
        {
            // 入队加锁
            std::lock_guard<std::mutex> lock(lg_tex);
            // push只接受一个参数用{}构造LogInfo结构体
            logque.push(li);
        }
        // 唤醒锁
        cv.notify_one();
    }

private:
    // 输出日志工作函数
    void worker()
    {
        while (run)
        {
            // 加锁取数据
            std::unique_lock<std::mutex> lock(lg_tex);
            // 队列中无数据等待休眠释放锁
            cv.wait(lock, [this]
                    { return logque.size() > 0 || !run; });
            while (logque.size() > 0)
            {
                LogInfo info = logque.front();
                logque.pop();
                lock.unlock();
                // 写文件放锁
                switch (info.method)
                {
                case SCREEN:
                    std::cout << info.print << std::endl;
                    break;
                case FILE:
                    write(fds[info.level], info.print.c_str(), info.print.size());
                    if (info.level == Fatal)
                        fsync(fds[info.level]);
                    break;
                }
                // 重新上锁
                lock.lock();
            }
        }
    }
    // 提前打开全部文件描述符避免打印一次日志就打开一次增加能耗
    void initFile()
    {
        mkdir(PATH, 0755);
        const char *name[] = {"Debug.log", "Info.log", "Warning.log", "Error.log", "Fatal.log"};
        for (int i = 0; i < FILENUM; i++)
        {
            std::string filename = std::string(PATH) + name[i];
            fds[i] = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        }
    }
    // 关闭文件描述符
    void closeFd()
    {
        for (int i = 0; i < FILENUM; i++)
        {
            close(fds[i]);
        }
    }

    std::string getLevel(int lv)
    {
        switch (lv)
        {
        case Debug:
            return "Debug";
        case Info:
            return "Info";
        case Warning:
            return "Warning";
        case Error:
            return "Error";
        case Fatal:
            return "Fatal";
        }
        return "None";
    }
};
inline Log &lg()
{
    static Log log;
    return log;
}