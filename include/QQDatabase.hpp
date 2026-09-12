#pragma once

#include <mysql/mysql.h>
#include <vector>
#include <memory>
#include <string>
#include <json/json.h>

#include "FastStmt.hpp"
#include "SeqAlloctor.hpp"
#include <format>

#define USERNOEXISTS 10000
#define USERPASSWORDNOEQUAL 10001
#define USERLOGINSUCCESS 10002

#define GETFRIENDSUCCESS 10003
#define GETGROUPSUCCESS 10004

#define MESSAGEINMYSQL 10005
#define MESSAGESENDSUCCESS 10006
#define MESSAGESENDFAILED 10007

#define ADDFRIENDMESSAGESUCCESS 10008

#define GETNAMESUCCESS 10009
#define REQUESTREPETITION 100010
#define ISFRIEND 10011
#define FRIENDAGREE 10012
#define FRIENDREFUSE 10013

#define NEWUSERNOOFFMES 10014
#define GETMESSUCCESS 10015
#define GETNOTICESUCCESS 10016

#define INTERCEPTPACKET 10017

#define GETSEQFAILED 10018

#define FRIENDCONVTYPE 0
#define GROUPCONVTYPE 1

// struct connMysql
// {
//     MYSQL *mysql;
//     time_t age;
//     // connMysql(){};
// };

// 通知类型
enum class NOTICETYPE : int8_t
{
    FRIEND = 1, // 好友通知
    GROUP = 2   // 群落通知
};

// 好友通知状态
enum class FriendNoticeStatus : int8_t
{
    PENDING = 1, // 待处理
    AGREE = 2,   // 同意
    REFUSE = 3   // 拒绝
};

class QQDatabase
{
private:
    struct MysqlDeleter
    {
        void operator()(struct MYSQL *mysql)
        {
            if (mysql)
                mysql_close(mysql);
        }
    };
    std::vector<std::unique_ptr<MYSQL, MysqlDeleter>> vmysql;
    // 句柄索引
    int mysqlidx;
    // 会话序列分配者
    SeqAlloctor convsac;
    // 用户通知序列分配者
    SeqAlloctor usersac;
    // 用户通知更新序列分配者
    SeqAlloctor syncsac;

public:
    QQDatabase(int threadnum)
    {
        mysqlidx = threadnum;
        for (int i = 0; i < mysqlidx; i++)
        {
            MYSQL *mysql = mysql_init(nullptr);
            if (mysql_real_connect(mysql, LOCAL_HOST, "root", "123456", "qqdatabase", 3306, nullptr, 0) == nullptr)
            {
                lg()(Error, SCREEN, "数据库连接失败!");
                exit(0);
            }
            std::unique_ptr<MYSQL, MysqlDeleter> temp(mysql);
            mysql_query(mysql, "set names utf8");
            vmysql.push_back(std::move(temp));
        }
        creatTable();
    }
    ~QQDatabase()
    {
    }
    // 处理好友申请结果
    unsigned int handleAddFriendResult(Json::Value &v, Json::Value &senderdata, Json::Value &recverdata, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 获取用户添加好友结果
        std::string result = v["result"].asString();
        // 请求主键
        long long requestkey = v["reuqestkey"].asInt64();
        // 通知状态
        int8_t status = static_cast<int8_t>(FriendNoticeStatus::PENDING);
        // 开启事物防止重复处理同意请求
        mysql_autocommit(mysql, 0);
        mysql_query(mysql, "START TRANSACTION;");
        // 预处理sql失败(获取请求者的id,账号,用户名,备注，分组，接收者的id,账号，用户名，备注分组均为默认值)
        if (!stmt.PrePareSql(R"(select r.userid,r.useraccount,r.username,a.verification,a.remark,a.identity,
                                 s.userid,s.useraccount,s.username 
                                 from addfriend_request a join qquser r on a.requestid=r.userid
                                 join qquser s on a.recvid=s.userid where requestkey=? AND status=?;)"))
        {
            // 错误回退
            mysql_rollback(mysql);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult select PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 请求者的id,账号,用户名,验证信息,对接受者的备注和分组
        long long rid = 0;
        char raccount[11] = {0};
        char rname[37] = {0};
        char verification[25536] = {0};
        char aremark[37] = {0};
        char aidentity[37] = {0};
        // 接收者的id,账号，用户名
        long long sid = 0;
        char saccount[11] = {0};
        char sname[37] = {0};
        // 绑定输出
        if (!stmt.BindResult(&rid, raccount, rname, verification, aremark, aidentity, &sid, saccount, sname))
        {
            // 错误回退
            mysql_rollback(mysql);
            // 重新开启自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult select BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&requestkey, &status))
        {
            // 错误回退
            mysql_rollback(mysql);
            // 重新开启自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult select Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            // 错误回退
            mysql_rollback(mysql);
            // 重新开启自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult select StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 读取数据
        int ret = stmt.FetchRow();
        // 没数据重复处理
        if (ret == MYSQL_NO_DATA)
        {
            // 错误回退
            mysql_rollback(mysql);
            // 重新开启自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult repetionhandlerequest");
            return -1;
        }
        // 出错
        else if (ret)
        {
            // 错误回退
            mysql_rollback(mysql);
            // 重新开启自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase handleAddFriendResult select FetchRow failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        std::cout << "raccount" << raccount << "\nsaccount" << saccount << std::endl;
        // 获取同步序列
        long long requestsyncseq = 0;
        requestsyncseq = syncsac.getSeq(rid);
        // 获取失败懒加载会话信息
        if (requestsyncseq == -1)
        {
            // 懒加载失败重试最多三次
            for (int i = 0; i < 3; i++)
            {
                requestsyncseq = getSyncSeq(rid, index);
                if (requestsyncseq != -1)
                {
                    break;
                }
            }
            // 三次全失败了
            if (requestsyncseq == -1)
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult getconvseq failed");
                return -1;
            }
            // 获取成功
            requestsyncseq = syncsac.initSeq(rid, requestsyncseq);
        }
        long long recvsyncseq = 0;
        recvsyncseq = syncsac.getSeq(sid);
        // 获取失败懒加载会话信息
        if (recvsyncseq == -1)
        {
            // 懒加载失败重试最多三次
            for (int i = 0; i < 3; i++)
            {
                recvsyncseq = getSyncSeq(sid, index);
                if (recvsyncseq != -1)
                {
                    break;
                }
            }
            // 三次全失败了
            if (recvsyncseq == -1)
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult getconvseq failed");
                return -1;
            }
            // 获取成功
            recvsyncseq = syncsac.initSeq(sid, recvsyncseq);
        }
        // 同意好友请求
        if (result == "同意")
        {
            // 会话类型
            int convtype = FRIENDCONVTYPE;
            // 执行sql语句(userid1为小的iduserid2为大的id)
            long long userid1 = std::min(rid, sid);
            long long userid2 = std::max(rid, sid);
            // 会话id
            long long convid = -1;
            // 会话id查重
            if (!stmt.PrePareSql("select convid from conversation where userid1=? AND userid2=?;"))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult checkrepetition PrePareSql failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 临时变量取convid
            long long temp = 0;
            if (!stmt.BindResult(&temp))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult checkrepetition BindResult failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            if (!stmt.Execute(&userid1, &userid2))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult checkrepetition Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            if (!stmt.StoreResult())
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult checkrepetition StoreResult failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 能读取到数据说明会话id已经存在
            int ret = stmt.FetchRow();
            if (ret == 0)
            {
                // 会话id
                convid = temp;
            }
            // 不存在创建会话id
            else if (ret == MYSQL_NO_DATA)
            {
                if (!stmt.PrePareSql("insert into conversation(userid1,userid2,convtype) values(?,?,?);"))
                {
                    // 错误回退
                    mysql_rollback(mysql);
                    // 重新开启自动提交
                    mysql_autocommit(mysql, 1);
                    lg()(Error, SCREEN, "QQDatabase handleAddFriendResult getconvid PrePareSql failed:{}", stmt.GetError());
                    return stmt.GetError();
                }
                if (!stmt.Execute(&userid1, &userid2, &convtype))
                {
                    // 错误回退
                    mysql_rollback(mysql);
                    // 重新开启自动提交
                    mysql_autocommit(mysql, 1);
                    lg()(Error, SCREEN, "QQDatabase handleAddFriendResult getconvid Execute failed:{}", stmt.GetError());
                    return stmt.GetError();
                }
                // 会话id
                convid = mysql_stmt_insert_id(stmt.GetStmt());
            }
            // 出错
            else
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult checkrepetition FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将双方分别插入好友表中
            if (!stmt.PrePareSql(R"(insert into qqfriend(userid,friendid,convid,friendaccount,friendname,friendremark,friendidentity) 
                                 values(?,?,?,?,?,?,?),
                                 (?,?,?,?,?,'无','我的好友');)"))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult insert PrePareSql failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 执行sql语句
            if (!stmt.Execute(&rid, &sid, &convid, saccount, sname, aremark, aidentity, &sid, &rid, &convid, raccount, rname))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult insert Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }

            // 插入完成更新addfriend_request和notification的status和用户的同步序列
            if (!stmt.PrePareSql(R"(update addfriend_request a join notification n on a.requestkey=n.bindid 
                                    set a.status=?,n.status=?,
                                    n.syncseq=CASE WHEN n.userid=? THEN ? ELSE ? END 
                                    where a.requestkey=? AND n.userid IN (?,?);)"))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult agreeupdate PrePareSql failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            status = static_cast<int8_t>(FriendNoticeStatus::AGREE);
            // 执行sql语句
            if (!stmt.Execute(&status, &status, &rid, &requestsyncseq, &recvsyncseq, &requestkey, &rid, &sid))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult agreeupdate Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            //  获取打招呼消息id和时间
            long long firstgreet = 0;
            long long secondgreet = 0;
            firstgreet = convsac.getSeq(convid);
            // 获取失败懒加载会话信息
            if (firstgreet == -1)
            {
                // 懒加载失败重试最多三次
                for (int i = 0; i < 3; i++)
                {
                    firstgreet = getConvSeq(convid, index);
                    if (firstgreet != -1)
                    {
                        break;
                    }
                }
                // 三次全失败了
                if (firstgreet == -1)
                {
                    // 错误回退
                    mysql_rollback(mysql);
                    // 重新开启自动提交
                    mysql_autocommit(mysql, 1);
                    lg()(Error, SCREEN, "QQDatabase handleAddFriendResult getconvseq failed");
                }
                // 获取成功
                firstgreet = convsac.initSeq(convid, firstgreet);
            }
            secondgreet = convsac.getSeq(convid);
            // 获取当前时间戳
            auto now = std::chrono::system_clock::now();
            long long sendtime = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            //  发送招呼信息（请求方为第一句接收方为第二句）
            if (!stmt.PrePareSql("insert into message(convid,convseq,sendid,content,sendtime,uuid) values(?,?,?,?,?,?),(?,?,?,'我同意了你的好友请求,我们可以开始聊天了!',?,?)"))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult greet PrePareSql failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 招呼标识uuid(用户id，请求id(bindid)，序列id)
            std::string firstuuid = std::to_string(rid) + "_greet_" + std::to_string(requestkey) + "_" + std::to_string(firstgreet);
            std::string seconduuid = std::to_string(sid) + "_greet_" + std::to_string(requestkey) + "_" + std::to_string(secondgreet);
            // 执行sql语句
            if (!stmt.Execute(&convid, &firstgreet, &rid, verification, &sendtime, &firstuuid, &convid, &secondgreet, &sid, &sendtime, &seconduuid))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult greet Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }

            // 提交事物重新开启自动提交
            mysql_commit(mysql);
            mysql_autocommit(mysql, 1);

            // 将数据存入json对象里面
            // 发送端收到同意请求
            senderdata["friendid"] = Json::Int64(sid);           // 好友id
            senderdata["convid"] = Json::Int64(convid);          // 好友会话id
            senderdata["bindid"] = Json::Int64(requestkey);      // 用户对应通知的bindid
            senderdata["syncseq"] = Json::Int64(requestsyncseq); // 用户通知同步序列id

            senderdata["mygreet"] = verification;              // 我的验证消息
            senderdata["myconvseq"] = Json::Int64(firstgreet); // 我的验证消息的会话序列

            senderdata["friendgreet"] = "我同意了你的好友请求,我们可以开始聊天了!"; // 好友招呼
            senderdata["friendconvseq"] = Json::Int64(secondgreet);                 // 好友招呼信息序列

            senderdata["sendtime"] = Json::Int64(sendtime); // 招呼消息发送时间

            senderdata["friendaccount"] = saccount;   // 好友账号
            senderdata["friendname"] = sname;         // 好友用户名
            senderdata["friendremark"] = aremark;     // 好友备注
            senderdata["friendidentity"] = aidentity; // 好友分组

            // 接收端同意成功
            recverdata["friendid"] = Json::Int64(rid);        // 好友id
            recverdata["convid"] = Json::Int64(convid);       // 好友会话id
            recverdata["bindid"] = Json::Int64(requestkey);   // 用户对应通知的bindid
            recverdata["syncseq"] = Json::Int64(recvsyncseq); // 用户通知同步序列id

            recverdata["mygreet"] = "我同意了你的好友请求,我们可以开始聊天了!"; // 我的招呼消息
            recverdata["myconvseq"] = Json::Int64(secondgreet);                 // 我的验证消息的会话序列

            recverdata["friendgreet"] = verification;              // 好友验证消息
            recverdata["friendconvseq"] = Json::Int64(firstgreet); // 好友验证消息序列

            recverdata["sendtime"] = Json::Int64(sendtime); // 好友验证消息发送时间

            recverdata["friendaccount"] = raccount;    // 好友账号
            recverdata["friendname"] = rname;          // 好友用户名
            recverdata["friendremark"] = "无";         // 好友备注
            recverdata["friendidentity"] = "我的好友"; // 好友分组
            return FRIENDAGREE;
        }
        // 拒绝好友请求
        else
        {
            // 插入完成更新addfriend_request的status
            if (!stmt.PrePareSql(R"(update addfriend_request a join notification n on a.requestkey=n.bindid 
                                    set a.status=?,n.status=?,
                                    n.syncseq=CASE WHEN n.userid=? THEN ? ELSE ? END 
                                    where a.requestkey=? AND userid IN (?,?);)"))
            {
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult refuseupdate PrePareSql failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            int8_t status = static_cast<int8_t>(FriendNoticeStatus::REFUSE);
            // 执行sql语句
            if (!stmt.Execute(&status, &status, &rid, &requestsyncseq, &recvsyncseq, &requestkey, &rid, &sid))
            {
                // 错误回退
                mysql_rollback(mysql);
                // 重新开启自动提交
                mysql_autocommit(mysql, 1);
                lg()(Error, SCREEN, "QQDatabase handleAddFriendResult refuseupdate Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 提交事物重新开启自动提交
            mysql_commit(mysql);
            mysql_autocommit(mysql, 1);
            recverdata["friendid"] = Json::Int64(rid);      // 好友id
            senderdata["bindid"] = Json::Int64(requestkey); // 用户对应通知的bindid
            recverdata["bindid"] = Json::Int64(requestkey); // 用户对应通知的bindid

            return FRIENDREFUSE;
        }
    }

    // 添加好友消息入库
    unsigned int pushAddFriendRequest(Json::Value &v, long long &requestid, Json::Value &requesterreply, Json::Value &recvdata, int &index)
    {
        // 获取mysql句柄
        MYSQL *mysql = getMysql(index);
        long long recvid = v["recvid"].asInt64();
        // 获取接收者的用户名和账号
        char recvaccount[11] = {0};
        char recvname[37] = {0};
        getUserAccountName(recvid, recvaccount, recvname, index);
        // 获取请求者者的用户名和账号
        char requestaccount[11] = {0};
        char requestname[37] = {0};
        getUserAccountName(requestid, requestaccount, requestname, index);
        // 初始化FastStmmt
        FastStmt stmt(mysql);
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase pushAddFriendRequest checkpending IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 第一次请求让请求入库
        // 开启事物
        mysql_query(mysql, "START TRANSACTION");
        // 关闭自动提交
        mysql_autocommit(mysql, 0);
        if (!stmt.PrePareSql(R"(insert into addfriend_request(requestid,recvid,verification,remark,identity,uuid,sendtime,status) 
                                 select ?,?,?,?,?,?,?,? 
                                 where 
                                 NOT EXISTS
                                 (select 1 from addfriend_request where requestid=? AND recvid=? AND status=?)
                                 AND NOT EXISTS
                                 (select 1 from qqfriend where userid=? AND friendid=?);)"))
        {
            lg()(Error, SCREEN, "QQDatabase pushAddFriendRequest insertrequest PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 验证信息
        std::string verification = v["verificationinfo"].asString();
        // 备注
        std::string remark = v["remark"].asString();
        // 分组
        std::string identity = v["ingroup"].asString();
        if (identity.empty())
        {
            identity = "无";
        }
        // uuid
        std::string uuid = v["uuid"].asString();
        // 获取当前时间戳
        auto now = std::chrono::system_clock::now();
        long long sendtime = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        // 通知状态
        int8_t status = static_cast<int8_t>(FriendNoticeStatus::PENDING);
        // 执行sql语句
        int ret = stmt.Execute(&requestid, &recvid, &verification, &remark, &identity, &uuid, &sendtime,
                               &status, &requestid, &recvid, &status, &requestid, &recvid);
        // 条件不满足说明已经是好友或重复提交申请
        if (mysql_affected_rows(mysql) == 0)
        {
            // 回滚
            mysql_rollback(mysql);
            // 重新自动提交
            mysql_autocommit(mysql, 1);
            return REQUESTREPETITION;
        }
        //  预处理sql语句(uuid重复 拦截了重复包)
        if (ret == 1062)
        {
            // 回滚
            mysql_rollback(mysql);
            // 重新自动提交
            mysql_autocommit(mysql, 1);
            return INTERCEPTPACKET;
        }
        else if (!ret)
        {
            // 回滚
            mysql_rollback(mysql);
            // 重新自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase pushAddFriendRequest insertrequest Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 获取最后一次插入也就是请求的主键id
        long long requestkey = mysql_stmt_insert_id(stmt.GetStmt());
        // 获取接收者用户通知序列
        long long recvseq = usersac.getSeq(recvid);
        // 没有信息懒加载获取用户通知信息插入哈希表
        if (recvseq == -1)
        {
            // 防止出错最多尝试三次
            for (int i = 0; i < 3; i++)
            {
                recvseq = getNoticeSeq(recvid, index);
                // 取到退出
                if (recvseq != -1)
                {
                    break;
                }
            }
            // 超过重试次数
            if (recvseq == -1)
            {
                // 回滚
                mysql_rollback(mysql);
                // 重新自动提交
                mysql_autocommit(mysql, 1);
                return GETSEQFAILED;
            }
            // 加到活跃通知用户哈希表内
            recvseq = usersac.initSeq(recvid, recvseq);
        }
        // 获取发送者用户通知序列
        long long requestseq = usersac.getSeq(requestid);
        // 没有信息懒加载获取用户通知信息插入哈希表
        if (requestseq == -1)
        {
            // 防止出错最多尝试三次
            for (int i = 0; i < 3; i++)
            {
                requestseq = getNoticeSeq(requestid, index);
                // 取到退出
                if (requestseq != -1)
                {
                    break;
                }
            }
            // 超过重试次数
            if (requestseq == -1)
            {
                // 回滚
                mysql_rollback(mysql);
                // 重新自动提交
                mysql_autocommit(mysql, 1);
                return GETSEQFAILED;
            }
            // 加到活跃通知用户哈希表内
            requestseq = usersac.initSeq(requestid, requestseq);
        }
        // 消息入通知库
        if (!stmt.PrePareSql("insert into notification(userid,userseq,bindid,type,status,payload,sendtime) values(?,?,?,?,?,?,?),(?,?,?,?,?,?,?);"))
        {
            // 回滚
            mysql_rollback(mysql);
            // 重新自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase pushAddFriendRequest insertnotice PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 通知类型
        int8_t type = static_cast<int8_t>(NOTICETYPE::FRIEND);

        // 请求者好友通知有效信息
        Json::Value requestpayload;
        requestpayload["sendid"] = Json::Int64(requestid);
        requestpayload["friendid"] = Json::Int64(recvid);
        requestpayload["friendaccount"] = recvaccount;
        requestpayload["friendname"] = recvname;
        requestpayload["verificationmes"] = verification;
        // 将json数据转换成字符串
        Json::FastWriter writer;
        std::string temp = writer.write(requestpayload);
        char requestjson_str[4096] = {0};
        // 防溢出
        strncpy(requestjson_str, temp.c_str(), temp.size());
        // 手动补0
        requestjson_str[temp.size() - 1] = '\0';

        // 接收者好友通知有效信息
        Json::Value recvpayload;
        recvpayload["sendid"] = Json::Int64(requestid);
        recvpayload["friendid"] = Json::Int64(requestid);
        recvpayload["friendaccount"] = requestaccount;
        recvpayload["friendname"] = requestname;
        recvpayload["verificationmes"] = verification;
        // 将json对象转换成字符串
        temp = writer.write(recvpayload);
        char recvjson_str[4096] = {0};
        // 防溢出
        strncpy(recvjson_str, temp.c_str(), temp.size());
        // 手动补0
        recvjson_str[temp.size() - 1] = '\0';
        // 将两条数据插入通知表
        if (!stmt.Execute(&recvid, &recvseq, &requestkey, &type, &status, recvjson_str, &sendtime, &requestid, &requestseq, &requestkey, &type, &status, requestjson_str, &sendtime))
        {
            // 回滚
            mysql_rollback(mysql);
            // 重新自动提交
            mysql_autocommit(mysql, 1);
            lg()(Error, SCREEN, "QQDatabase pushAddFriendRequest insertnotice Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 成功commit开启自动提交
        mysql_commit(mysql);
        mysql_autocommit(mysql, 1);
        // 填充json数据
        // 发送者通知信息
        requesterreply["userid"] = Json::Int64(requestid);
        requesterreply["userseq"] = Json::Int64(requestseq);
        requesterreply["bindid"] = Json::Int64(requestkey);
        requesterreply["type"] = type;
        requesterreply["payload"] = requestpayload;
        requesterreply["status"] = status;
        requesterreply["sendtime"] = Json::Int64(sendtime);

        // 接受者通知信息
        recvdata["userid"] = Json::Int64(recvid);
        recvdata["userseq"] = Json::Int64(recvseq);
        recvdata["bindid"] = Json::Int64(requestkey);
        recvdata["type"] = type;
        recvdata["payload"] = recvpayload;
        recvdata["status"] = status;
        recvdata["sendtime"] = Json::Int64(sendtime);

        return ADDFRIENDMESSAGESUCCESS;
    }

    // 接收信息成功(更新信息状态为成功)
    unsigned int recvMessageSuccess(Json::Value &v, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase recvMessageSuccess IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql失败
        if (!stmt.PrePareSql("update message set status=success where recvid=? and content=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase recvMessageSuccess PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        std::string recvid = v["recvid"].asString();
        std::string content = v["content"].asString();
        // 执行sql语句
        if (!stmt.Execute(&recvid, &content))
        {
            lg()(Error, SCREEN, "QQDatabase recvMessageSuccess Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 消息状态更改成功
        return MESSAGESENDSUCCESS;
    }
    // 添加信息
    unsigned int addMessage(Json::Value &v, long long sendid, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase addMessage IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql失败
        if (!stmt.PrePareSql("insert into message(sendid,recvid,content) values(?,?,?);"))
        {
            lg()(Error, SCREEN, "QQDatabase addMessage PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        std::string recvid = v["recvid"].asString();
        std::string content = v["content"].asString();
        // 执行sql语句
        if (!stmt.Execute(&sendid, &recvid, &content))
        {
            lg()(Error, SCREEN, "QQDatabase addMessage Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 消息入库成功
        return MESSAGEINMYSQL;
    }

    // 处理搜索信息
    bool handleSearch(Json::Value &searchinfo, long long &userid, std::string &account, int &index)
    {
        // 获取用户好友信息
        unsigned int ret = getFriend(searchinfo["friendinfo"], userid, account, index);
        // 获取好友信息失败打印错误码
        if (ret != GETFRIENDSUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase handleSearch getFriend failed:{}", ret);
            return false;
        }
        // 获取好友群聊信息
        ret = getGroup(searchinfo["groupinfo"], account, index);
        if (ret != GETGROUPSUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase handleSearch getGroup failed:{}", ret);
            return false;
        }
        return true;
    }

    // 获取好友通知增量
    unsigned int getNoticeMes(Json::Value &noticearray, long long &id, long long &userseq, long long &syncseq, int &index)
    {
        int ret = getNotice(noticearray, id, userseq, syncseq, index);
        if (ret != GETNOTICESUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeMes getNotice failed:{}", ret);
            return ret;
        }
        return ret;
    }

    // 获取好友离线消息
    unsigned int getOfflineMes(Json::Value &convinfo, Json::Value &offmesarray, int &index)
    {
        // 如果用户是新用户没有会话信息则直接返回
        if (convinfo.empty())
        {
            return NEWUSERNOOFFMES;
        }
        int ret = getMes(convinfo, offmesarray, index);
        if (ret != GETMESSUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase getOfflineMes getMes failed:{}", ret);
            return ret;
        }
        // 返回
        return ret;
    }

    // 获取用户好友、群聊数据
    bool getUserData(Json::Value &userinfo, long long &userid, int &index)
    {
        // 获取用户好友信息
        unsigned int ret = getFriend(userinfo["friendlist"], userid, index);
        // 获取好友信息失败打印错误码
        if (ret != GETFRIENDSUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase getUserData getFriend failed:{}", ret);
            return false;
        }
        // 获取好友群聊信息
        ret = getGroup(userinfo["grouplist"], userid, index);
        if (ret != GETGROUPSUCCESS)
        {
            lg()(Error, SCREEN, "QQDatabase getUserData getGroup failed:{}", ret);
            return false;
        }
        return true;
    }

    // 处理登入信息
    unsigned int handleLogin(Json::Value &v, long long &userid, char *username, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql失败
        if (!stmt.PrePareSql("select userpassword,userid,username from qquser where useraccount=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        char password[49] = {0};
        char name[37] = {0};
        // 绑定传出参数
        if (!stmt.BindResult(password, &userid, name))
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        std::string useraccount = v["useraccount"].asString();
        // 执行sql语句
        if (!stmt.Execute(&useraccount))
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 读取一行数据
        int ret = stmt.FetchRow();
        // 用户不存在（没有数据）
        if (ret == MYSQL_NO_DATA)
        {
            return USERNOEXISTS;
        }
        // 不为0有报错读取数据失败
        if (ret)
        {
            lg()(Error, SCREEN, "QQDatabase handleLogin FetchRow failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 密码错误
        if (strcmp(password, v["userpassword"].asString().c_str()) != 0)
        {
            userid = -1;
            return USERPASSWORDNOEQUAL;
        }
        // 用户名
        strncpy(username, name, 37);
        return USERLOGINSUCCESS;
    }

    // 处理注册信息
    unsigned int handleRegister(Json::Value &v, std::string &account, int &idx)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(idx);
        // MYSQLSTMT封装函数
        FastStmt stmt(mysql);
        // 初始化是否完成
        if (!stmt.IsInit())
        {
            lg()(Warning, SCREEN, "QQDatabase handleRegister IsInit failed:{}", mysql_error(mysql));
            return mysql_errno(mysql);
        }
        std::string username = v["username"].asString();
        std::string userpassword = v["userpassword"].asString();
        char useraccount[24] = {0};
        // 看用户有没有用户名
        if (!v["username"].asString().empty())
        {
            std::string username = v["username"].asString();
            // 预处理sql语句
            if (!stmt.PrePareSql("insert into qquser (useraccount,userpassword,username) values(?,?,?);"))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister PrePareSql failed:{}", mysql_error(mysql));
                return stmt.GetError();
            }
            if (!stmt.Execute(useraccount, &userpassword, &username))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            long long userid = mysql_stmt_insert_id(stmt.GetStmt());
            snprintf(useraccount, 24, "%010lld", userid + 1000000000);
            // 预处理sql语句
            if (!stmt.PrePareSql("update qquser set useraccount=? where userid=?"))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister PrePareSql failed:{}", mysql_error(mysql));
                return stmt.GetError();
            }
            if (!stmt.Execute(useraccount, &userid))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
        }
        else
        {
            // 预处理sql语句
            if (!stmt.PrePareSql("insert into qquser (useraccount,userpassword) values(?,?);"))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister PrePareSql failed:{}", mysql_error(mysql));
                return stmt.GetError();
            }
            if (!stmt.Execute(useraccount, &userpassword))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 拼接账号
            long long userid = mysql_stmt_insert_id(stmt.GetStmt());
            snprintf(useraccount, 24, "%010lld", userid + 1000000000);
            // 预处理sql语句
            if (!stmt.PrePareSql("update qquser set useraccount=? where userid=?"))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister PrePareSql failed:{}", mysql_error(mysql));
                return stmt.GetError();
            }
            if (!stmt.Execute(useraccount, &userid))
            {
                lg()(Warning, SCREEN, "QQDatabase handleRegister Execute failed:{}", stmt.GetError());
                return stmt.GetError();
            }
        }
        // 获取账号
        account = useraccount;
        return 0;
    }

    // 获取用户名
    unsigned int getUserAccountName(long long &userid, char *useraccount, char *username, int &index)
    {
        // 获取数据库句柄
        MYSQL *mysql = getMysql(index);
        // MYSQLSTMT封装函数
        FastStmt stmt(mysql);
        // 初始化是否完成
        if (!stmt.IsInit())
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName IsInit failed:{}", mysql_error(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select useraccount,username from qquser where userid=?"))
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        char account[11] = {0};
        char name[109] = {0};
        // 绑定字段（传出参数）
        if (!stmt.BindResult(account, name))
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&userid))
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将结果拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 读取一行数据
        if (stmt.FetchRow())
        {
            lg()(Warning, SCREEN, "QQDatabase getUserName FetchRow failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将获取到的账号和用户名赋值
        strncpy(username, name, 36);
        strncpy(useraccount, account, 10);
        return GETNAMESUCCESS;
    }

private:
    // 获取MYSQL句柄
    MYSQL *getMysql(int index)
    {
        MYSQL *mysql = vmysql.at(index).get();
        // 连接断开重连
        if (mysql_ping(mysql) != 0)
        {
            MYSQL *temp = mysql_init(nullptr);
            if (mysql_real_connect(temp, LOCAL_HOST, "root", "123456", "qqdatabase", 3306, nullptr, 0) == nullptr)
            {
                lg()(Error, SCREEN, "数据库重连失败：", mysql_errno(temp));
                exit(1);
            }
            mysql_query(temp, "set names utf8");
            // 换活跃的句柄
            vmysql.at(index).reset(temp);
        }
        return vmysql.at(index).get();
    }
    // 获取离线信息
    unsigned int getMes(Json::Value &convinfo, Json::Value &offmesarray, int &index)
    {
        // 获取MYSQL句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getMes IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 获取会话id和会话最大序列(取会话id中序列大于客户端最大会话序列的所有消息)
        std::string sql = "select mes.mesid,mes.convid,mes.convseq,mes.sendid,mes.content,mes.sendtime from message AS mes join (";
        int i = 1;
        // 构建临时表(所有会话id和会话序列)
        for (auto convvalue : convinfo)
        {
            long long convid = convvalue["convid"].asInt64();
            long long lastseq = convvalue["lastseq"].asInt64();
            // 拼接sql预计构建会话信息临时表
            if (int i = 1)
                sql += "select " + std::to_string(convid) + " AS convid ," + std::to_string(lastseq) + " AS convseq";
            else
                sql += "UNION ALL select " + std::to_string(convid) + " AS convid ," + std::to_string(lastseq) + " AS convseq";
            i++;
        }
        // 构建完继续完善sql语句
        sql += ") AS tempconvinfo on mes.convid=tempconvinfo.convid AND mes.convseq>tempconvinfo.convseq";
        if (!stmt.PrePareSql(sql.c_str()))
        {
            lg()(Error, SCREEN, "QQDatabase getMes PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        long long mesid = -1;
        long long convid = -1;
        long long convseq = -1;
        long long sendid = -1;
        char content[25536] = {0};
        long long sendtime = 0;
        if (!stmt.BindResult(&mesid, &convid, &convseq, &sendid, content, &sendtime))
        {
            lg()(Error, SCREEN, "QQDatabase getMes BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        if (!stmt.Execute())
        {
            lg()(Error, SCREEN, "QQDatabase getMes Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getMes StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 循环读取离线消息
        int ret = 10086;
        while (1)
        {
            ret = stmt.FetchRow();
            // 读取完毕退出
            if (ret == MYSQL_NO_DATA)
            {
                break;
            }
            // 出错
            if (ret != 0)
            {
                lg()(Error, SCREEN, "QQDatabase getMes StoreResult failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将消息放到json对象里面
            Json::Value mes;
            mes["mesid"] = Json::Int64(mesid);
            mes["convid"] = Json::Int64(convid);
            mes["convseq"] = Json::Int64(convseq);
            mes["sendid"] = Json::Int64(sendid);
            mes["content"] = content;
            mes["sendtime"] = Json::Int64(sendtime);
            // 将消息追加到json数组里面
            offmesarray.append(mes);
        }
        // 返回
        return GETMESSUCCESS;
    }
    // 获取通知增量
    unsigned int getNotice(Json::Value &noticearray, long long &id, long long &lastuserseq, long long &lastsyncseq, int &index)
    {
        MYSQL *mysql = getMysql(index);
        FastStmt stmt(mysql);
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getNotice IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 获取通知信息
        if (!stmt.PrePareSql("select userid,userseq,syncseq,bindid,type,status,payload,sendtime from notification where userid=? AND (userseq>? OR syncseq>?);"))
        {
            lg()(Error, SCREEN, "QQDatabase getNotice PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 通知拥有者
        long long userid = 0;
        // 通知序列
        long long userseq = 0;
        // 同步序列
        long long syncseq = 0;
        // 关联的请求id
        long long bindid = 0;
        // 通知类型
        int8_t type = 0;
        // 通知状态
        int8_t status = 0;
        // 通知有效数据
        char payload[4096] = {0};
        // 通知发送时间
        long long sendtime = 0;
        if (!stmt.BindResult(&userid, &userseq, &syncseq, &bindid, &type, &status, payload, &sendtime))
        {
            lg()(Error, SCREEN, "QQDatabase getNotice BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        if (!stmt.Execute(&id, &lastuserseq, &lastsyncseq))
        {
            lg()(Error, SCREEN, "QQDatabase getNotice Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getNotice StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        int ret = 10086;
        // 将payload字符串转换成json对象
        Json::Reader reader;
        // 循环读取通知增量
        while (1)
        {
            // 读取一行通知
            ret = stmt.FetchRow();
            // 读取完毕退出
            if (ret == MYSQL_NO_DATA)
            {
                break;
            }
            // 出错
            if (ret != 0)
            {
                lg()(Error, SCREEN, "QQDatabase getNotice FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            Json::Value data;
            data["userid"] = Json::Int64(userid);
            data["userseq"] = Json::Int64(userseq);
            data["syncseq"] = Json::Int64(syncseq);
            data["bindid"] = Json::Int64(bindid);
            data["type"] = type;
            data["status"] = status;
            // 将payload字符串转换成json对象
            Json::Value jpayload;
            reader.parse(payload, jpayload);
            data["payload"] = jpayload;
            data["sendtime"] = Json::Int64(sendtime);
            noticearray.append(data);
        }
        return GETNOTICESUCCESS;
    }
    // 获取会话消息序列
    long long getConvSeq(long long &id, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        // FastStmt初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq IsInit failed:{}", mysql_errno(mysql));
            return -1;
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select IFNULL(MAX(convseq),0) from message where convid=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq PrePareSql failed:{}", stmt.GetError());
            return -1;
        }
        long long convseq = 0;
        // 绑定传出参数
        if (!stmt.BindResult(&convseq))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq BindResult failed:{}", stmt.GetError());
            return -1;
        }
        // 执行sql语句
        if (!stmt.Execute(&id))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq Execute failed:{}", stmt.GetError());
            return -1;
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq StoreResult failed:{}", stmt.GetError());
            return -1;
        }
        // 读取数据
        // 读取一行数据
        int ret = stmt.FetchRow();
        // 出错
        if (ret)
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq FetchRow failed:{}", stmt.GetError());
            return -1;
        }
        return convseq;
    }
    // 获取用户同步序列
    long long getSyncSeq(long long &id, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        // FastStmt初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq IsInit failed:{}", mysql_errno(mysql));
            return -1;
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select IFNULL(MAX(syncseq),0) from notification where userid=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq PrePareSql failed:{}", stmt.GetError());
            return -1;
        }
        long long syncseq = 0;
        // 绑定传出参数
        if (!stmt.BindResult(&syncseq))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq BindResult failed:{}", stmt.GetError());
            return -1;
        }
        // 执行sql语句
        if (!stmt.Execute(&id))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq Execute failed:{}", stmt.GetError());
            return -1;
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq StoreResult failed:{}", stmt.GetError());
            return -1;
        }
        // 读取数据
        // 读取一行数据
        int ret = stmt.FetchRow();
        // 出错
        if (ret)
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq FetchRow failed:{}", stmt.GetError());
            return -1;
        }
        return syncseq;
    }
    // 获取用户通知序列
    long long getNoticeSeq(long long &id, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        // FastStmt初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq IsInit failed:{}", mysql_errno(mysql));
            return -1;
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select IFNULL(MAX(userseq),0) from notification where userid=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq PrePareSql failed:{}", stmt.GetError());
            return -1;
        }
        long long userseq = 0;
        // 绑定传出参数
        if (!stmt.BindResult(&userseq))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq BindResult failed:{}", stmt.GetError());
            return -1;
        }
        // 执行sql语句
        if (!stmt.Execute(&id))
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq Execute failed:{}", stmt.GetError());
            return -1;
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq StoreResult failed:{}", stmt.GetError());
            return -1;
        }
        // 读取数据
        // 读取一行数据
        int ret = stmt.FetchRow();
        // 出错
        if (ret)
        {
            lg()(Error, SCREEN, "QQDatabase getNoticeSeq FetchRow failed:{}", stmt.GetError());
            return -1;
        }
        return userseq;
    }
    // 获取用户群聊信息
    unsigned int getGroup(Json::Value &groupinfo, std::string &account, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        // FastStmt初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getGroup IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select groupid,groupaccount,groupname from qqgroup where groupaccount like '%?%';"))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        long long groupid = 0;
        char groupaccount[11] = {0};
        char groupname[109] = {0};
        // 绑定传出参数
        if (!stmt.BindResult(&groupid, groupaccount, groupname))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&account))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getGroup StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        int ret = 10086;
        // 读取数据
        while (1)
        {
            // 读取一行数据
            ret = stmt.FetchRow();
            // 没数据退出
            if (ret == MYSQL_NO_DATA)
                break;
            // ret不为0说明出错了
            if (ret != 0)
            {
                lg()(Error, SCREEN, "QQDatabase getGroup FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将数据读取到json对象中追加进json数组里面
            Json::Value v;
            v["groupid"] = Json::Int64(groupid);
            v["groupaccount"] = groupaccount;
            v["groupname"] = groupname;
            std::cout << 2 << std::endl;
            groupinfo.append(v);
        }
        return GETGROUPSUCCESS;
    }
    // 获取用户好友信息
    unsigned int getFriend(Json::Value &friendinfo, long long &userid, std::string &account, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getFriend IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql失败 (不返回用户本身)
        if (!stmt.PrePareSql("select userid,useraccount,username from qquser where useraccount LIKE CONCAT('%',?,'%') and userid!=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        long long friendid = 0;
        char friendaccount[11] = {0};
        char friendname[109] = {0};
        // 绑定传出参数
        if (!stmt.BindResult(&friendid, friendaccount, friendname))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&account, &userid))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getFriend StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        int ret = 10086;
        // 读取数据
        while (1)
        {
            // 读取一行数据
            ret = stmt.FetchRow();
            // 没数据退出
            if (ret == MYSQL_NO_DATA)
                break;
            // 不为0有报错读取数据失败
            if (ret)
            {
                lg()(Error, SCREEN, "QQDatabase getFriend FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将好友数据追加到json数组后面
            Json::Value v;
            v["friendid"] = Json::Int64(friendid);
            v["friendaccount"] = friendaccount;
            v["friendname"] = friendname;
            friendinfo.append(v);
        }
        return GETFRIENDSUCCESS;
    }
    // 获取用户群聊信息
    unsigned int getGroup(Json::Value &grouplist, long long &userid, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建FastStmt对象
        FastStmt stmt(mysql);
        // FastStmt初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getGroup IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql语句
        if (!stmt.PrePareSql("select g.groupid,g.convid,g.groupaccount,g.groupname,g.groupremark,m.useridentity from qqgroupmem m inner join qqgroup g on g.groupid=m.groupid where userid=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        long long groupid = 0;
        long long convid = 0;
        char groupaccount[11] = {0};
        char groupname[109] = {0};
        char groupremark[109] = {0};
        char useridentity[109] = {0};
        // 绑定传出参数
        if (!stmt.BindResult(&groupid, &convid, groupaccount, groupname, groupremark, useridentity))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&userid))
        {
            lg()(Error, SCREEN, "QQDatabase getGroup Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getGroup StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        int ret = 10086;
        // 读取数据
        while (1)
        {
            // 读取一行数据
            ret = stmt.FetchRow();
            // 没数据退出
            if (ret == MYSQL_NO_DATA)
                break;
            // ret不为0说明出错了
            if (ret)
            {
                lg()(Error, SCREEN, "QQDatabase getGroup FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将数据读取到json对象中追加进json数组里面
            Json::Value v;
            v["groupid"] = Json::Int64(groupid);
            v["convid"] = Json::Int64(convid);
            v["groupaccount"] = groupaccount;
            v["groupname"] = groupname;
            v["groupremark"] = groupremark;
            v["useridentity"] = useridentity;
            grouplist.append(v);
        }
        return GETGROUPSUCCESS;
    }
    // 获取用户好友信息
    unsigned int getFriend(Json::Value &friendlist, long long &userid, int &index)
    {
        // 获取句柄
        MYSQL *mysql = getMysql(index);
        // 创建Faststmt对象
        FastStmt stmt(mysql);
        // 初始化失败
        if (!stmt.IsInit())
        {
            lg()(Error, SCREEN, "QQDatabase getFriend IsInit failed:{}", mysql_errno(mysql));
            return mysql_errno(mysql);
        }
        // 预处理sql失败
        if (!stmt.PrePareSql("select friendid,convid,friendaccount,friendname,friendremark,friendidentity from qqfriend where userid=?;"))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend PrePareSql failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        long long friendid = 0;
        long long convid = 0;
        char friendaccount[37] = {0};
        char friendname[109] = {0};
        char friendremark[109] = {0};
        char friendidentity[109] = {0};
        // 绑定传出参数
        if (!stmt.BindResult(&friendid, &convid, friendaccount, friendname, friendremark, friendidentity))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend BindResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 执行sql语句
        if (!stmt.Execute(&userid))
        {
            lg()(Error, SCREEN, "QQDatabase getFriend Execute failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        // 将数据拉到本地
        if (!stmt.StoreResult())
        {
            lg()(Error, SCREEN, "QQDatabase getFriend StoreResult failed:{}", stmt.GetError());
            return stmt.GetError();
        }
        int ret = 10086;
        // 读取数据
        while (1)
        {
            // 读取一行数据
            ret = stmt.FetchRow();
            // 没数据退出
            if (ret == MYSQL_NO_DATA)
                break;
            // 不为0有报错读取数据失败
            if (ret)
            {
                lg()(Error, SCREEN, "QQDatabase getFriend FetchRow failed:{}", stmt.GetError());
                return stmt.GetError();
            }
            // 将好友数据追加到json数组后面
            Json::Value v;
            v["friendid"] = Json::Int64(friendid);
            v["convid"] = Json::Int64(convid);
            v["friendaccount"] = friendaccount;
            v["friendname"] = friendname;
            v["friendremark"] = friendremark;
            v["friendidentity"] = friendidentity;
            friendlist.append(v);
        }
        return GETFRIENDSUCCESS;
    }

    // 创建数据表
    void creatTable()
    {
        MYSQL *mysql = getMysql(0);
        // 创建用户表
        if (mysql_query(mysql, R"(create table if not exists qquser(
                    userid       BIGINT AUTO_INCREMENT PRIMARY KEY          COMMENT '用户id',
                    useraccount  VARCHAR(10) NOT NULL UNIQUE             COMMENT '用户账号',
                    userpassword VARCHAR(16) NOT NULL                    COMMENT '用户密码',
                    username     VARCHAR(36) NOT NULL DEFAULT '企鹅用户'  COMMENT '用户昵称',
                    INDEX IDX_UID(userid),
                    INDEX IDX_UAC(useraccount)
                    );)"))
        {
            exit(2);
        }
        // 创建群表
        if (mysql_query(mysql, R"(create table if not exists qqgroup(
                        groupid      BIGINT      AUTO_INCREMENT PRIMARY KEY    COMMENT '群id',
                        convid       BIGINT      NOT NULL UNIQUE               COMMENT '群会话id',
                        groupaccount VARCHAR(36) NOT NULL UNIQUE               COMMENT '群号',
                        groupname    VARCHAR(36) NOT NULL                      COMMENT '群名',
                        groupremark  VARCHAR(36) NOT NULL                      COMMENT '群备注',
                        userownerid  BIGINT      NOT NULL                      COMMENT '群主',
                        INDEX IDX_OWNER(userownerid)
                        );)"))
        {
            exit(2);
        }
        // 创建好友表
        if (mysql_query(mysql, R"(CREATE TABLE IF NOT EXISTS qqfriend(
                        userid         BIGINT      PRIMARY KEY NOT NULL       COMMENT '用户id',
                        friendid       BIGINT      NOT NULL                   COMMENT '好友id',
                        convid         BIGINT      NOT NULL                   COMMENT '好友会话id',
                        friendaccount  VARCHAR(10) NOT NULL                   COMMENT '好友账号',
                        friendname     VARCHAR(36) NOT NULL                   COMMENT '好友昵称',
                        friendremark   VARCHAR(36) NOT NULL DEFAULT '无'      COMMENT '好友备注',
                        friendidentity VARCHAR(36) NOT NULL DEFAULT '我的好友' COMMENT '好友身份',
                        INDEX IDX_FRIENDID(friendid)
                        );)"))
        {
            exit(2);
        }
        // 创建群成员表
        if (mysql_query(mysql, R"(create table if not exists qqgroupmem(
                        groupid      BIGINT      NOT NULL                 COMMENT '群id',
                        userid       BIGINT      NOT NULL                 COMMENT '用户id',
                        useridentity VARCHAR(36) NOT NULL DEFAULT '群成员' COMMENT '用户群身份',
                        PRIMARY KEY (groupid,userid),
                        INDEX IDX_UID(userid)
                        );
                        )"))
        {
            exit(2);
        }
        // 消息表
        if (mysql_query(mysql, R"(create table if not exists message(
                        mesid    BIGINT        AUTO_INCREMENT PRIMARY KEY         COMMENT '消息主键id',
                        convid   BIGINT        NOT NULL                           COMMENT '会话id',
                        convseq  BIGINT        NOT NULL                           COMMENT '会话消息序列',
                        sendid   BIGINT        NOT NULL                           COMMENT '发送者id',
                        content  VARCHAR(8192) NOT NULL                           COMMENT '消息内容',
                        sendtime BIGINT        NOT NULL                           COMMENT '发送时间',
                        uuid     char(36)      NOT NULL UNIQUE                    COMMENT 'uuid防重复插入',
                        INDEX idx_convinfo(convid,convseq)                        COMMENT '会话id和序列索引'
                        );)"))
        {
            exit(2);
        }
        // 会话表（0私聊 1群聊)
        if (mysql_query(mysql, R"(create table if not exists conversation(
                        convid     BIGINT   AUTO_INCREMENT PRIMARY KEY         COMMENT '会话主键',
                        userid1    BIGINT                                      COMMENT '会话拥有者1',
                        userid2    BIGINT                                      COMMENT '会话拥有者2',
                        convtype   TINYINT  NOT NULL                           COMMENT '会话类型',
                        createtime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                        groupid    BIGINT                                      COMMENT '群聊id',
                        UNIQUE KEY uk(userid1,userid2)                         COMMENT '防止同一会话重复插入',
                        INDEX idx(userid1,userid2)
                        );)"))
        {
            exit(2);
        }
        // 通知消息表
        if (mysql_query(mysql, R"(create table if not exists notification(
                        noticeid   BIGINT  AUTO_INCREMENT PRIMARY KEY    COMMENT '通知主键',
                        userid     BIGINT  NOT NULL                      COMMENT '通知拥有者',
                        userseq    BIGINT  NOT NULL                      COMMENT '用户通知序列',
                        syncseq    BIGINT  NOT NULL DEFAULT 0            COMMENT '同步更新序列',
                        bindid     BIGINT  NOT NULL                      COMMENT '关联的请求id',
                        type       TINYINT NOT NULL                      COMMENT '1 好友 2 群聊',
                        payload    JSON    NOT NULL                      COMMENT '处理通知的信息',
                        status     TINYINT NOT NULL                      COMMENT '通知状态',
                        sendtime   BIGINT  NOT NULL                      COMMENT '发送时间',
                        INDEX idx_recv(userid,userseq DESC,syncseq DESC) COMMENT '通知所有者的id'
                        );)"))
        {
            exit(2);
        }
        // 添加好友表
        if (mysql_query(mysql, R"(create table if not exists addfriend_request(
                        requestkey     BIGINT        AUTO_INCREMENT PRIMARY KEY    COMMENT '请求主键',
                        requestid      BIGINT        NOT NULL                      COMMENT '请求者id',
                        recvid         BIGINT        NOT NULL                      COMMENT '接受者id',
                        verification   VARCHAR(8192) NOT NULL                      COMMENT '验证消息',
                        remark         VARCHAR(36)   NOT NULL DEFAULT '无'         COMMENT  '备注',
                        identity       VARCHAR(36)   NOT NULL DEFAULT '我的好友'    COMMENT  '好友分组',
                        uuid           char(36)      NOT NULL UNIQUE               COMMENT 'uuid防重复插入',
                        sendtime       BIGINT        NOT NULL                      COMMENT '发送时间',
                        status         TINYINT       NOT NULL                      COMMENT '请求状态',
                        INDEX idx_recv(requestid,recvid,status)                    COMMENT '获取数据和防止间隙锁锁整表'
                        );)"))
        {
            exit(2);
        }
        // 好友事件表  eventtype:"new_mes" "friend_request" "friend_reply" "group_invit" "group_create"
        // if (mysql_query(mysql, R"(create table if not exists user_event(
        //                 eventid   INT         AUTO_INCREMENT PRIMARY KEY           COMMENT '事件主键',
        //                 userid    INT         NOT NULL                             COMMENT '事件拥有者id',
        //                 eventtype VARCHAR(36) NOT NULL                             COMMENT '事件类型',
        //                 content   JSON        NOT NULL                             COMMENT '事件内容',
        //                 status    VARCHAR(36) NOT NULL DEFAULT 'pending'           COMMENT '事件状态',
        //                 eventtime DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP   COMMENT '事件时间',
        //                 INDEX idx_recv(userid,status,eventtime DESC)                             COMMENT '收到请求按时间倒序排序'
        //                 );)"))
        // {
        //     exit(2);
        // }
    }
};