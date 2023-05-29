# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

mutable struct PoolManager{T} <: AbstractPoolManager{<:AbstractConnectionManager}
    conns::Channel{T} # channel
    numactive::Int # number of active connections
    max::Int  # maximum number of active connections
    bind_key::String # 属于哪个bind_key
    dbconf::DBConfig  # mysql connection configuration
    dbkwargs::Base.Pairs # mysql kwargs for connection
end

"""存储在channel中的链接 """
mutable struct ConnectionManager <: AbstractConnectionManager
    conn::MySQL.Connection  # MySQL connection
    idle::Float64  # 空闲时间
    bind_key::String # 属于哪个bind_key
    recycle::Int # 空闲时长关闭
end

""" 创建连接池链接"""
function create_poolconn(pm::PoolManager{<:AbstractConnectionManager}, conf::MysqlDBConfig; kwargs...)
    conn::MySQL.Connection = DBInterface.connect(
        MySQL.Connection, conf.dbhost, conf.username, conf.password;
        db = conf.dbname, port = conf.port, reconnect = conf.reconnect, kwargs...)
    # 设置是否自动提交默认否
    MySQL.API.autocommit(conn.mysql, conf.autocommit)
    # 加入channel
    put!(pm.conns, ConnectionManager(conn, time(), pm.bind_key, conf.pool_recycle))
end

function Base.acquire(bind_key::String = "default")::ConnectionManager
    pm::PoolManager{ConnectionManager} = dbpools[bind_key]
    lock(pm.conns) do 
        while isready(pm.conns)
            conn::ConnectionManager = take!(pm.conns)
            if (time() - conn.idle) > conn.recycle
                DBInterface.close!(conn.conn)
                pm.numactive -= 1
            elseif !isopen(conn.conn)
                DBInterface.close!(conn.conn)
                pm.numactive -= 1
            else
                return conn
            end
        end
        # If there are not too many connections, create new
        if pm.numactive < pm.max
            create_poolconn(pm, pm.dbconf; dbkwargs)
            pm.numactive += 1
        end
        # otherwise, wait for a connection to be released
        while true
            conn::ConnectionManager = take!(pm.conns)
            if (time() - conn.idle) > conn.recycle
                DBInterface.close!(conn.conn)
                pm.numactive -= 1
            elseif !isopen(conn.conn)
                DBInterface.close!(conn.conn)
                pm.numactive -= 1
            else
                return conn
            end
            if pm.numactive < pm.max
                create_poolconn(pm, pm.dbconf; dbkwargs)
                pm.numactive += 1
            end
        end
    end
end

function Base.release(conn::ConnectionManager)
    pm = dbpools[conn.bind_key]
    lock(pm.conns) do 
        conn.idle = time()
        # 加入channel
        put!(pm.conns, conn)
        pm.numactive += 1
    end
end

function with_connection(f::Function, bind_key::String = "default")
    conn::ConnectionManager = Base.acquire(bind_key)
    try
        return f(conn.conn)
    finally
        Base.release(conn)
    end
end
