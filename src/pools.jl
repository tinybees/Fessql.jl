# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

mutable struct ConnectionPod{T} <: Pod{<:PoolConnection}
    conns::Channel{T} # channel
    numactive::Int # number of active connections
    max::Int  # maximum number of active connections
    bind::String # 属于哪个bind
    dbconf::DBConfig
    dbkwargs::Base.Pairs
end

"""存储在channel中的链接 """
mutable struct ConnectionManager <: PoolConnection
    conn::MySQL.Connection  # MySQL connection
    idle::Float64  # 空闲时间
    bind::String # 属于哪个bind
    recycle::Int # 空闲时长关闭
end

""" 创建连接池链接"""
function create_poolconn(pod::ConnectionPod{<:PoolConnection}, conf::MysqlDBConfig; kwargs...)
    conn::MySQL.Connection = DBInterface.connect(
        MySQL.Connection, conf.dbhost, conf.username, conf.password;
        db = conf.dbname, port = conf.port, reconnect = conf.reconnect, kwargs...)
    # 设置是否自动提交默认否
    MySQL.API.autocommit(conn.mysql, conf.autocommit)
    # 加入channel
    put!(pod.conns, ConnectionManager(conn, time(), pod.bind, conf.pool_recycle))
end

function Base.acquire(bind::String = "default")::ConnectionManager
    pod::ConnectionPod{ConnectionManager} = dbconns[bind]
    lock(pod.conns)
    try
        while isready(pod.conns)
            conn::ConnectionManager = take!(pod.conns)
            if (time() - conn.idle) > conn.recycle
                DBInterface.close!(conn.conn)
                pod.numactive -= 1
            elseif !isopen(conn.conn)
                DBInterface.close!(conn.conn)
                pod.numactive -= 1
            else
                return conn
            end
        end
        # If there are not too many connections, create new
        if pod.numactive < pod.max
            create_poolconn(pod, pod.dbconf; dbkwargs)
            pod.numactive += 1
        end
        # otherwise, wait for a connection to be released
        while true
            conn::ConnectionManager = take!(pod.conns)
            if (time() - conn.idle) > conn.recycle
                DBInterface.close!(conn.conn)
                pod.numactive -= 1
            elseif !isopen(conn.conn)
                DBInterface.close!(conn.conn)
                pod.numactive -= 1
            else
                return conn
            end
            if pod.numactive < pod.max
                create_poolconn(pod, pod.dbconf; dbkwargs)
                pod.numactive += 1
            end
        end
    finally
        unlock(pod.conns)
    end
end

function Base.release(conn::ConnectionManager)
    pod = dbconns[conn.bind]
    lock(pod.conns)
    try
        conn.idle = time()
        # 加入channel
        put!(pod.conns, conn)
        pod.numactive += 1
    finally
        unlock(pod.conns)
    end
end

function with_connection(f::Function, bind::String = "default")
    conn::ConnectionManager = Base.acquire(bind)
    try
        return f(conn.conn)
    finally
        Base.release(conn)
    end
end
