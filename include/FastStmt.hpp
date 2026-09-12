#include <mysql/mysql.h>
#include <cstring>
#include <vector>
#include <string>
#include <iostream>

class FastStmt
{

private:
    MYSQL_STMT *stmt;
    std::vector<MYSQL_BIND> vpushbind;
    std::vector<MYSQL_BIND> vpopbind;
    std::vector<unsigned long> vlen; // 保护传出mysql写缓存区
    bool push = true;
    bool pop = false;

public:
    FastStmt(MYSQL *mysql) : stmt(mysql_stmt_init(mysql)) {}
    // 关闭句柄
    ~FastStmt()
    {
        // 释放资源  如果push为false说明绑定了字段传出参数肯定也将执行了stroeresult
        if (pop)
            mysql_stmt_free_result(stmt);
        // 关闭句柄
        if (stmt)
            mysql_stmt_close(stmt);
    }
    FastStmt(const FastStmt &) = delete;
    FastStmt &operator=(const FastStmt &) = delete;
    // 预处理sql语句
    bool PrePareSql(const char *sql)
    {
        // 如果复用了则下一次预处理之前先释放上一次的资源
        if (pop)
        {
            mysql_stmt_free_result(stmt);
            pop = false;
        }
        return mysql_stmt_prepare(stmt, sql, strlen(sql)) == 0;
    }
    // 执行sql语句
    template <typename... Args>
    bool Execute(Args &&...args)
    {
        // 清空上次绑定的数据
        vpushbind.clear();
        // 设置为入参模式
        push = true;
        // 清空三次绑定的参数
        mysql_stmt_reset(stmt);
        // 绑定入参
        BindAll(std::forward<Args>(args)...);
        if (!vpushbind.empty())
            if (mysql_stmt_bind_param(stmt, vpushbind.data()) != 0)
                return false;
        return mysql_stmt_execute(stmt) == 0;
    }
    // 绑定传出参数
    template <typename... Args>
    bool BindResult(Args &&...args)
    {
        // 清空上次绑定的参数
        vpopbind.clear();
        vlen.clear();
        // 先开辟好内存大小防止lenpushback换位置length变成野指针
        vlen.reserve(sizeof...(args));
        // 设为传出模式
        push = false;
        // 绑定了作标记要释放缓存区
        pop = true;
        // 绑定传出参数
        BindAll(std::forward<Args>(args)...);
        return mysql_stmt_bind_result(stmt, vpopbind.data()) == 0;
    }
    // 拉取数据到本地
    bool StoreResult() { return mysql_stmt_store_result(stmt) == 0; }
    // 读取一行数据
    int FetchRow() { return mysql_stmt_fetch(stmt); }
    // 获取错误码
    unsigned int GetError() { return mysql_stmt_errno(stmt); }
    // 获取stmt
    MYSQL_STMT *GetStmt() { return stmt; }
    // 是否初始化成功
    bool IsInit() { return stmt != nullptr; }

private:
    // 根据情况绑定参数
    template <typename T, typename... Args>
    void BindAll(T &&first, Args &&...args)
    {
        if (push)
        {
            BindPush(first);
        }
        else
        {
            BindPop(first);
        }
        BindAll(std::forward<Args>(args)...);
    }
    // 无参退出
    void BindAll() {}
    // 入参绑定int
    void BindPush(int *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_LONG;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        vpushbind.push_back(bind);
    }
    // 入参绑定int8_t
    void BindPush(int8_t *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_TINY;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        vpushbind.push_back(bind);
    }
    // 传出绑定int
    void BindPop(int *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        unsigned long length = 0;
        vlen.push_back(length);
        bind.buffer_type = MYSQL_TYPE_LONG;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        bind.length = &(vlen.back());
        vpopbind.push_back(bind);
    }
    // 传出绑定int8_t
    void BindPop(int8_t *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        unsigned long length = 0;
        vlen.push_back(length);
        bind.buffer_type = MYSQL_TYPE_TINY;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        bind.length = &(vlen.back());
        vpopbind.push_back(bind);
    }
    // 传入绑定long long
    void BindPush(long long *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_LONGLONG;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        vpushbind.push_back(bind);
    }
    // 传出绑定long long(count(*)返回的是BIGINT)
    void BindPop(long long *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        unsigned long length = 0;
        vlen.push_back(length);
        bind.buffer_type = MYSQL_TYPE_LONGLONG;
        bind.buffer = data;
        bind.buffer_length = sizeof(*data);
        bind.length = &(vlen.back());
        vpopbind.push_back(bind);
    }
    // 入参绑定char*
    void BindPush(const char *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_STRING;
        bind.buffer = const_cast<char *>(data);
        bind.buffer_length = strlen(data);
        vpushbind.push_back(bind);
    }
    // 入参绑定string
    void BindPush(const std::string *data)
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_STRING;
        bind.buffer = const_cast<char *>(data->data());
        bind.buffer_length = data->size();
        vpushbind.push_back(bind);
    }
    // 传出绑定string
    void BindPop(std::string *data)
    {
        // MYSQL_BIND bind;
        // memset(&bind,0,sizeof(bind));
        // unsigned long length=0;
        // vlen.push_back(length);
        // bind.buffer_type=MYSQL_TYPE_STRING;
        // bind.buffer=data->data();
        // bind.buffer_length=data->capacity();
        // bind.length=&(vlen.back());
        // vpopbind.push_back(bind);
    }
    // 传出绑定char[]
    template <int NUM>
    void BindPop(char (&data)[NUM])
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        unsigned long length = 0;
        vlen.push_back(length);
        bind.buffer_type = MYSQL_TYPE_STRING;
        bind.buffer = data;
        bind.buffer_length = NUM;
        bind.length = &(vlen.back());
        vpopbind.push_back(bind);
    }
    // 传入char[]
    template <int NUM>
    void BindPush(char (&data)[NUM])
    {
        MYSQL_BIND bind;
        memset(&bind, 0, sizeof(bind));
        bind.buffer_type = MYSQL_TYPE_STRING;
        bind.buffer = data;
        bind.buffer_length = strlen(data);
        vpushbind.push_back(bind);
    }
};