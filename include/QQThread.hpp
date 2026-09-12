#pragma once

#include <event2/event.h>
#include <thread>
#include <unistd.h>
#include <event2/util.h>
#include <event2/buffer.h>
#include <arpa/inet.h>
#include <json/json.h>
#include <unordered_map>
#include <ctime>

#include "Log.hpp"
#include "QQDatabase.hpp"
#include "QQInfo.hpp"

#define EVBUFFERADDFAILED 10000
#define BUFFEREVENTWRITEFAILED 10001
#define SENDSUCCESS 10002

class QQThread
{
private:
    QQDatabase *qqdb;
    struct event_base *m_base;
    std::thread t;
    int curindex;
    QQInfo *qqinfo;

public:
    QQThread(QQDatabase *db, QQInfo *temp, int index)
    {
        // 获取数据库对象
        qqdb = db;
        // 确认自己的线程下标
        curindex = index;
        // 获取QQInfo
        qqinfo = temp;
        // 创建事件集合
        m_base = event_base_new();
        // 让事件集合在子线程中阻塞
        t = std::thread(&QQThread::worker, this);
    }
    ~QQThread()
    {
        if (m_base)
            event_base_loopbreak(m_base);
        if (t.joinable())
            t.join();
        if (m_base)
            event_base_free(m_base);
    }

    /// 工作函数
    void worker()
    {
        // 设置3秒定时器
        struct timeval timer = {3, 0};
        // 设置回调函数归属flag(用于事件集合唤醒)
        struct event *timer_out = event_new(m_base, -1, EV_PERSIST, time_cb, this);
        // 将定时器属性加到事件中
        event_add(timer_out, &timer);
        // 开启事件循环
        event_base_dispatch(m_base);
    }

    // 唤醒函数
    static void time_cb(int i, short s, void *ctx) {};

    struct event_base *getBase()
    {
        return m_base;
    }

    // 读回调
    static void readCallBack(struct bufferevent *bev, void *ctx)
    {
        QQThread *thread = static_cast<QQThread *>(ctx);

        char buf[65536] = {0};
        // 循环读取数据
        while (thread->StickyWrapHelper(bev, buf))
        {
            Json::Reader reader;
            Json::Value value;
            if (!reader.parse(buf, value))
            {
                lg()(Error, SCREEN, "QQThread readCallBack parse failed:{}", reader.getFormattedErrorMessages());
                // 单包错误先执行其他的
                continue;
            }
            // 处理命令
            // 用户注册
            if (value["cmd"] == "register")
            {
                thread->registeruser(bev, value);
            }
            // 用户登入
            else if (value["cmd"] == "login")
            {
                thread->loginuser(bev, value);
            }
            // 用户成功登入获取数据
            else if (value["cmd"] == "user_loginsuccess")
            {
                thread->sendUserData(bev);
            }
            // 用户获取离线消息
            else if (value["cmd"] == "getofflinemes")
            {
                thread->getOfflineMes(bev, value);
            }
            // 用户获取通知增量
            else if (value["cmd"] == "getnoticemes")
            {
                thread->getNoticeMes(bev, value);
            }
            // 搜索好友或群
            else if (value["cmd"] == "search")
            {
                thread->searchInfo(bev, value);
            }
            // 添加好友
            else if (value["cmd"] == "addfriend")
            {
                thread->AddFriend(bev, value);
            }
            // 用户同意添加好友
            else if (value["cmd"] == "handlefriendrequest")
            {
                thread->AddFriendResult(bev, value);
            }
            // 发送消息
            else if (value["cmd"] == "sendmessage")
            {
                thread->sendFriendMessage(bev, value);
            }
            // 客户端收到消息
            else if (value["cmd"] == "recvmessuccess")
            {
                thread->qqdb->recvMessageSuccess(value, thread->curindex);
            }
        }
    }
    // 写回调
    static void writeCallBack(struct bufferevent *bev, void *ctx) {}
    // 事件回调
    static void eventCallBack(struct bufferevent *bev, short what, void *ctx)
    {
        // 事件掩码为用户正常断开或进程终止移出在线hash
        if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR))
        {
            QQThread *thread = static_cast<QQThread *>(ctx);
            // 加锁操作
            std::unique_lock<std::mutex> lock(thread->qqinfo->online_mutex);
            auto it = thread->qqinfo->bev_toid.find(bev);
            if (it != thread->qqinfo->bev_toid.end())
            {
                // 获取用户id
                long long userid = it->second;
                // 移除在线hash容器
                thread->qqinfo->bev_toid.erase(bev);
                thread->qqinfo->onlineuser.erase(userid);
                lg()(Info, SCREEN, "用户{}断开连接", userid);
                lock.unlock();
                bufferevent_free(bev);
            }
        }
    };

    // 发送好友消息
    void sendFriendMessage(struct bufferevent *bev, Json::Value &v)
    {
        // 获取发送者id 査全局hash上锁
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        long long sendid = (*qqinfo->bev_toid.find(bev)).second;
        // 用完迭代器解锁
        lock.unlock();
        // 获取会话id
        long long convid = v["convid"].asInt64();
        // 获取会话锁id(算哈希值取摸)
        int lockindex = std::hash<int64_t>{}(convid) % 512;
        // 上锁发消息保证会话消息按入库顺序串行发送
        // std::unique_lock<std::mutex> convlock(qqinfo->convlock[lockindex]);
        // 消息入库(入库失败)
        if (qqdb->addMessage(v, sendid, curindex) != MESSAGEINMYSQL)
        {
            Json::Value sendmessagedata;
            sendmessagedata["cmd"] = "sendmessage_reply";
            sendmessagedata["result"] = "sendmessage_failed";
            int sret = SendJson(bev, sendmessagedata);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendFriendMessage SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendFriendMessage SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 入库成功
        else
        {
            // 获取接收者id
            long long recvid = v["recvid"].asInt64();
            // 上锁查数据
            lock.lock();
            // 查看用户在不在线
            auto user = qqinfo->onlineuser.find(recvid);
            // 用户在线(在线发送消息不在线不作处理)
            if (user != qqinfo->onlineuser.end())
            {
                // 拿到接收用户的bev(这里我们在main.cc里面设置了evthread_use_pthreads所以在这里是线程安全的)
                struct bufferevent *recvbev = user->second;
                Json::Value sendmessagedata;
                sendmessagedata["cmd"] = "friend_message";
                sendmessagedata["sendid"] = Json::Int64(sendid);
                sendmessagedata["content"] = v["content"].asString();
                int sret = SendJson(recvbev, sendmessagedata);
                if (sret == EVBUFFERADDFAILED)
                {
                    lg()(Error, SCREEN, "QQThread sendFriendMessage SendJson EVBUFFERADDFAILED");
                }
                else if (sret == BUFFEREVENTWRITEFAILED)
                {
                    lg()(Error, SCREEN, "QQThread sendFriendMessage SendJson BUFFEREVENTWRITEFAILED");
                }
                // 发完数据后再解锁防止被释放
                lock.unlock();
            }
        }
        // 发送完消息在解锁(消息压入缓存区)
        // convlock.unlock();
    }
    // 处理添加好友请求结果
    void AddFriendResult(struct bufferevent *bev, Json::Value &v)
    {
        // 发给双方的数据
        Json::Value senderdata;
        Json::Value recverdata;
        senderdata["cmd"] = "addfriend_reply";
        recverdata["cmd"] = "addfriend_reply";

        int ret = qqdb->handleAddFriendResult(v, senderdata, recverdata, curindex);
        // 接收端同意好友请求给双方发送对应数据
        if (ret == FRIENDAGREE)
        {
            // 设置命令数据已经在handleAddFriendResult里面插入好了
            senderdata["result"] = "addfriend_agree";
            recverdata["result"] = "addfriend_agree";
        }
        // 接收端拒绝好友请求给双方发送数据
        else if (ret == FRIENDREFUSE)
        {
            senderdata["result"] = "123456";
            recverdata["result"] = "addfriend_refuse";
        }
        // 出错给接收端报错
        else
        {
            recverdata["result"] = "addfriend_handlefailed";
            int sret = SendJson(bev, recverdata);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriendResult SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriendResult SendJson BUFFEREVENTWRITEFAILED");
            }
            return;
        }
        // 获取发送请求端的id和bev(接收端的好友)
        long long sendid = recverdata["friendid"].asInt64();
        // 加锁获取bev
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        auto sendit = qqinfo->onlineuser.find(sendid);
        if (sendit != qqinfo->onlineuser.end())
        {
            // 在线发送数据直接发给发送端
            int sret = SendJson(sendit->second, senderdata);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriendResult SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriendResult SendJson BUFFEREVENTWRITEFAILED");
            }
            // 发完解锁
            lock.unlock();
        }
        // 将数据发送给接收端
        int sret = SendJson(bev, recverdata);
        if (sret == EVBUFFERADDFAILED)
        {
            lg()(Error, SCREEN, "QQThread AddFriendResult SendJson EVBUFFERADDFAILED");
        }
        else if (sret == BUFFEREVENTWRITEFAILED)
        {
            lg()(Error, SCREEN, "QQThread AddFriendResult SendJson BUFFEREVENTWRITEFAILED");
        }
    }
    // 添加好友请求
    void AddFriend(struct bufferevent *bev, Json::Value &v)
    {
        // 获取当前用户的id
        // 加锁操作
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        auto it = qqinfo->bev_toid.find(bev);
        if (it == qqinfo->bev_toid.end())
        {
            return;
        }
        long long sendid = it->second;
        // 拿到后解锁
        lock.unlock();
        // 判断用户用户是否添加的自己（搜索那边不会发用户本身数据但是保险多多安全多多）
        long long recvid = v["recvid"].asInt64();
        if (recvid == sendid)
        {
            Json::Value reply;
            reply["cmd"] = "addfriend_reply";
            reply["result"] = "addfriend_addoneself";
            int sret = SendJson(bev, reply);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson BUFFEREVENTWRITEFAILED");
            }
            return;
        }
        // 请求者回复数据
        Json::Value requesterreply;
        // 接收者恢复信息
        Json::Value recvdata;
        // 添加好友请求入库
        int ret = qqdb->pushAddFriendRequest(v, sendid, requesterreply, recvdata, curindex);
        // 重复请求不做处理或拦截重复包不做处理
        if (ret == REQUESTREPETITION || ret == INTERCEPTPACKET)
        {
            return;
        }
        // 入库失败
        else if (ret != ADDFRIENDMESSAGESUCCESS)
        {
            requesterreply["cmd"] = "addfriend_reply";
            requesterreply["result"] = "addfriend_failed";
            int sret = SendJson(bev, requesterreply);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 入库成功
        else
        {
            // 拿到接收方的bev
            // 加锁操作
            lock.lock();
            // 看看用户在不在线在线发送消息
            auto recvit = qqinfo->onlineuser.find(recvid);
            // 在线发送消息
            if (recvit != qqinfo->onlineuser.end())
            {
                struct bufferevent *recvbev = recvit->second;
                recvdata["cmd"] = "addfriend_request";
                // 将数据发给用户
                int sret = SendJson(recvbev, recvdata);
                if (sret == EVBUFFERADDFAILED)
                {
                    lg()(Error, SCREEN, "QQThread AddFriend SendJson EVBUFFERADDFAILED");
                }
                else if (sret == BUFFEREVENTWRITEFAILED)
                {
                    lg()(Error, SCREEN, "QQThread AddFriend SendJson BUFFEREVENTWRITEFAILED");
                }
            }
            // 发完解锁
            lock.unlock();
            // 告诉用户发送成功
            requesterreply["cmd"] = "addfriend_reply";
            requesterreply["result"] = "addfriend_success";
            int sret = SendJson(bev, requesterreply);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread AddFriend SendJson BUFFEREVENTWRITEFAILED");
            }
        }
    }
    // 搜索数据
    void searchInfo(struct bufferevent *bev, Json::Value &v)
    {
        // 获取账号信息
        std::string account = v["accountinfo"].asString();
        // json对象存放好友和群聊json数组
        Json::Value searchinfo;
        Json::Value friendinfo(Json::arrayValue);
        Json::Value groupinfo(Json::arrayValue);
        searchinfo["friendinfo"] = friendinfo;
        searchinfo["groupinfo"] = groupinfo;
        // 加锁操作获取用户的id
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        auto userit = qqinfo->bev_toid.find(bev);
        // 用户突然下线
        if (userit == qqinfo->bev_toid.end())
        {
            return;
        }
        long long userid = userit->second;
        // 拿到数据解锁
        lock.unlock();
        // 获取数据成功将数据发送给用户
        if (qqdb->handleSearch(searchinfo, userid, account, curindex))
        {
            searchinfo["cmd"] = "search_reply";
            searchinfo["result"] = "search_success";
            int sret = SendJson(bev, searchinfo);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread searchInfo SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread searchInfo SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 失败去除json数组回复
        else
        {
            searchinfo["cmd"] = "search_reply";
            searchinfo["result"] = "search_failed";
            searchinfo.removeMember("friendinfo");
            searchinfo.removeMember("groupinfo");
            int sret = SendJson(bev, searchinfo);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread searchInfo SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread searchInfo SendJson BUFFEREVENTWRITEFAILED");
            }
        }
    }
    // 获取通知增量
    void getNoticeMes(struct bufferevent *bev, Json::Value &v)
    {
        // 上锁获取用户id
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        auto it = qqinfo->bev_toid.find(bev);
        // 用户已下线
        if (it == qqinfo->bev_toid.end())
        {
            return;
        }
        long long userid = it->second;
        // 拿到数据解锁
        lock.unlock();
        // 用户通知序列
        long long userseq = v["noticeseq"].asInt64();
        long long syncseq = v["syncseq"].asInt64();
        // 通知增量json数组
        Json::Value noticemes(Json::arrayValue);
        // 获取通知增量
        int ret = qqdb->getNoticeMes(noticemes, userid, userseq, syncseq, curindex);
        if (ret == GETNOTICESUCCESS)
        {
            // 没有数据不返回
            if (noticemes.empty())
            {
                return;
            }
            Json::Value data;
            data["cmd"] = "getnoticemes_reply";
            data["noticemes"] = noticemes;
            // 将离线信息打包发送过去
            int sret = SendJson(bev, data);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread getNoticeMes SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread getNoticeMes SendJson BUFFEREVENTWRITEFAILED");
            }
        }
    }
    // 获取离线消息
    void getOfflineMes(struct bufferevent *bev, Json::Value &v)
    {
        // 离线信息json数组
        Json::Value offlinemes(Json::arrayValue);
        // 用户会话信息
        Json::Value convinfo(Json::arrayValue);
        convinfo = v["convinfo"];
        std::cout << v["convinfo"].isArray() << std::endl;
        // 获取用户离线消息
        int ret = qqdb->getOfflineMes(convinfo, offlinemes, curindex);
        // 获取完毕
        if (ret == GETMESSUCCESS)
        {
            Json::Value data;
            data["cmd"] = "getofflinemes_reply";
            data["offlinemes"] = offlinemes;
            // 将离线信息打包发送过去
            int sret = SendJson(bev, data);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread getOfflineMes SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread getOfflineMes SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 用户是新用户没有会话信息
        else if (ret == NEWUSERNOOFFMES)
        {
            return;
        }
        // 出错
        else
        {
            lg()(Error, SCREEN, "QQThread getOfflineMes failed{}", ret);
        }
    }

    // 将好友、群聊数据发给用户
    void sendUserData(struct bufferevent *bev)
    {
        // 用户数据为一个json对象包含了好友和群聊json数组，getUserFata将数据通过json对象的方式分别写入json数组中
        Json::Value userdata;
        Json::Value friendlist(Json::arrayValue);
        Json::Value grouplist(Json::arrayValue);
        Json::Value meslist(Json::arrayValue);
        Json::Value noticelist(Json::arrayValue);
        userdata["friendlist"] = friendlist;
        userdata["grouplist"] = grouplist;
        // 加锁操作取出用户id
        std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
        auto userit = qqinfo->bev_toid.find(bev);
        if (userit == qqinfo->bev_toid.end())
        {
            return;
        }
        long long userid = userit->second;
        // 取到数据解锁
        lock.unlock();
        // 获取数据成功将数据发送给用户
        if (qqdb->getUserData(userdata, userid, curindex))
        {
            userdata["cmd"] = "user_loginsuccess_reply";
            userdata["result"] = "getuserdata_success";
            int sret = SendJson(bev, userdata);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendUserData SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendUserData SendJson BUFFEREVENTWRITEFAILED");
            }
            // 获取用户关联的会话id信息和会话关联的用户id
            for (auto row : friendlist)
            {
                // 会话id不存在则原地构造存在获取对应迭代器
                auto cit = qqinfo->conv_touid.emplace(row["convid"].asInt64(), std::unordered_set<long long>{});
                cit.first->second.emplace(userid);
                auto uit = qqinfo->uid_toconnv.emplace(userid, std::unordered_set<long long>{});
                uit.first->second.emplace(row["convid"].asInt64());
            }
            for (auto row : grouplist)
            {
                // 会话id不存在则原地构造存在获取对应迭代器
                auto cit = qqinfo->conv_touid.emplace(row["convid"].asInt64(), std::unordered_set<long long>{});
                cit.first->second.emplace(userid);
                auto uit = qqinfo->uid_toconnv.emplace(userid, std::unordered_set<long long>{});
                uit.first->second.emplace(row["convid"].asInt64());
            }
        }
        // 失败去除json数组回复
        else
        {
            userdata["cmd"] = "user_loginsuccess_reply";
            userdata["result"] = "getuserdata_failed";
            userdata.removeMember("friendlist");
            userdata.removeMember("grouplist");
            int sret = SendJson(bev, userdata);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendUserData SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread sendUserData SendJson BUFFEREVENTWRITEFAILED");
            }
        }
    }

    // 用户登入
    void loginuser(struct bufferevent *bev, Json::Value &v)
    {
        long long userid;
        char username[37] = {0};
        unsigned int ret = qqdb->handleLogin(v, userid, username, curindex);
        if (ret == USERLOGINSUCCESS)
        {
            // 查数据上锁
            std::unique_lock<std::mutex> lock(qqinfo->online_mutex);
            auto it = qqinfo->onlineuser.find(userid);
            if (it != qqinfo->onlineuser.end())
            {
                // 用完迭代器释放
                lock.unlock();
                // 找到了说明用户已经在其他地方登入了告诉该用户
                Json::Value data;
                data["cmd"] = "login_reply";
                data["result"] = "login_online";
                int sret = SendJson(bev, data);
                if (sret == EVBUFFERADDFAILED)
                {
                    lg()(Error, SCREEN, "QQThread loginuser SendJson EVBUFFERADDFAILED");
                }
                else if (sret == BUFFEREVENTWRITEFAILED)
                {
                    lg()(Error, SCREEN, "QQThread loginuser SendJson BUFFEREVENTWRITEFAILED");
                }
            }
            else
            {
                // 账号不在线登入成功
                Json::Value value;
                value["cmd"] = "login_reply";
                value["result"] = "login_success";
                value["userid"] = Json::Int64(userid);
                value["username"] = username;
                int sret = SendJson(bev, value);
                if (sret == SENDSUCCESS)
                {
                    // 将登入用户标记为在线
                    qqinfo->onlineuser.insert({userid, bev});
                    // 反查找插入
                    qqinfo->bev_toid.insert({bev, userid});
                    // 写完释放
                    lock.unlock();
                }
                else if (sret == EVBUFFERADDFAILED)
                {
                    lg()(Error, SCREEN, "QQThread loginuser SendJson EVBUFFERADDFAILED");
                }
                else if (sret == BUFFEREVENTWRITEFAILED)
                {
                    lg()(Error, SCREEN, "QQThread loginuser SendJson BUFFEREVENTWRITEFAILED");
                }
                lg()(Info, SCREEN, "用户{}上线", userid);
            }
        }
        // 密码错误
        else if (ret == USERPASSWORDNOEQUAL)
        {
            Json::Value value;
            value["cmd"] = "login_reply";
            value["result"] = "user_worningpassword";
            int sret = SendJson(bev, value);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread loginuser SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread loginuser SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 用户还没注册
        else if (ret == USERNOEXISTS)
        {
            Json::Value value;
            value["cmd"] = "login_reply";
            value["result"] = "user_accountnoexists";
            int sret = SendJson(bev, value);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread loginuser SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread loginuser SendJson BUFFEREVENTWRITEFAILED");
            }
        }
    }
    // 注册用户
    void registeruser(struct bufferevent *bev, Json::Value &v)
    {
        std::string username = v["username"].asString();
        std::string useraccount;
        // 处理注册信息
        unsigned int ret = qqdb->handleRegister(v, useraccount, curindex);
        // 注册成功
        if (ret == 0)
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "register_sucess";
            value["useraccount"] = useraccount;
            int sret = SendJson(bev, value);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson EVBUFFERADDFAILED");
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson BUFFEREVENTWRITEFAILED");
            }
        }
        // 用户已存在
        else if (ret == 1062)
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "useraccount_exists";
            int sret = SendJson(bev, value);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson EVBUFFERADDFAILED");
                return;
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson BUFFEREVENTWRITEFAILED");
                return;
            }
        }
        // 注册失败
        else
        {
            Json::Value value;
            value["cmd"] = "register_reply";
            value["result"] = "register_failed";
            int sret = SendJson(bev, value);
            if (sret == EVBUFFERADDFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson EVBUFFERADDFAILED");
                return;
            }
            else if (sret == BUFFEREVENTWRITEFAILED)
            {
                lg()(Error, SCREEN, "QQThread registeruser SendJson BUFFEREVENTWRITEFAILED");
                return;
            }
        }
    }

    // 发送数据
    int SendJson(struct bufferevent *bev, Json::Value &v)
    {
        Json::FastWriter writer;
        std::string data = writer.write(v);
        int len = data.size();
        struct evbuffer *send = evbuffer_new();
        if (evbuffer_add(send, &len, 4) != 0)
        {
            lg()(Error, SCREEN, "QQThread SendJson evbuffer_add failed");
            return EVBUFFERADDFAILED;
        }
        if (evbuffer_add(send, data.data(), data.size()) != 0)
        {
            lg()(Error, SCREEN, "QQThread SendJson evbuffer_add failed");
            return EVBUFFERADDFAILED;
        }
        // 将数据直接移动写到bev的缓存区内部
        if (bufferevent_write_buffer(bev, send) != 0)
        {
            lg()(Error, SCREEN, "QQThread SendJson bufferevent_write_buffer failed");
            return BUFFEREVENTWRITEFAILED;
        }
        evbuffer_free(send);
        return SENDSUCCESS;
    }
    // 处理粘包问题（不能用线程级的buf当缓存区因为一旦分包读取数据失败可能会有残留数据无法清除，
    // 比如第一个包存入第二个包失败（断网）会导致第一个包的数据无法清除所以要用对象级别的，
    // 还有就是多个套接字往一个缓存区里面写数据可能导致数据混乱（第一个套接字还剩一个半包，第二个套接子接手发来一个新包数据混乱））
    bool StickyWrapHelper(struct bufferevent *bev, char *packet)
    {
        // 获取bev的读缓存区内容（不取出来）
        struct evbuffer *buf = bufferevent_get_input(bev);

        int read_len = evbuffer_get_length(buf);
        // 数据长度小于4报头大小不够
        if (read_len < 4)
            return false;
        // 数据大小
        uint32_t size;
        // 拷贝前四字节内存
        evbuffer_copyout(buf, &size, 4);
        if (size == 0 || size > 65536)
        {
            lg()(Fatal, SCREEN, "QQThread StickyWrapHelper failed overflow");
            bufferevent_free(bev);
            return false;
        }
        // 正文内容不够
        if (read_len < size + 4)
            return false;
        // 将数据包给到packet
        bufferevent_read(bev, &size, 4);
        bufferevent_read(bev, packet, size);
        return true;
    }
};