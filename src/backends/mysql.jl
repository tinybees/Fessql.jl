# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

using Tables

Base.@kwdef struct MysqlDBConfig <: DBConfig
    dbhost::String
    username::String
    password::String
    port::Int = 3306
    dbname::String
    reconnect::Bool=true
    autocommit::Bool=false
end

function initialize_db(conf::MysqlDBConfig, db_binds::Union{Nothing,Dict{String,MysqlDBConfig}}=nothing; kwargs...)
    try
        # 默认的库连接
        dbconns["default"] = DBInterface.connect(
            MySQL.Connection, conf.dbhost, conf.username, conf.password;
            db=conf.dbname, port=conf.port, reconnect=conf.reconnect, kwargs...)
        # 设置是否自动提交默认否
        MySQL.API.autocommit(dbconns["default"].mysql, conf.autocommit)
        # db_binds中的连接
        if db_binds !== nothing
            for (name, dbargs) in db_binds
                dbconns[name] = DBInterface.connect(
                    MySQL.Connection, dbargs.dbhost, dbargs.username, dbargs.password;
                    db=dbargs.dbname, port=dbargs.port, reconnect=dbargs.reconnect, kwargs...)
                # 设置是否自动提交默认否
                MySQL.API.autocommit(dbconns[name].mysql, dbargs.autocommit)
            end
        end
    catch ex
        @error ex
    end
    @info "初始化所有的数据库连接成功"
end

function initialize_db(conf::Dict{String,Any},db_binds::Union{Nothing,Dict{String,Any}}=nothing; kwargs...)
    conf2 = MysqlDBConfig(conf...)
    db_binds2 = Dict(name=>MysqlDBConfig(one_conf...) for (name, one_conf) in db_binds)
    initialize_db(conf2, db_binds2, kwargs...)
end

function close_db()
    try
        for (_, conn) in dbconns
            DBInterface.close!(conn)
        end
    catch ex
        @error ex
    end
    @info "关闭所有的数据库连接成功"
end

function tx_context(f::Function, conn::MySQL.Connection)->Any
    try
        DBInterface.execute(conn, "begin")
        result = f()
        MySQL.API.commit(conn.mysql)
        return result
    catch ex
        MySQL.API.rollback(conn.mysql)
        rethrow(ex)
    end
end

function execute(conn::MySQL.Connection, sql::AbstractString) -> Tables.dictrowtable
    sql = MySQL.escape(conn, sql)
    textcursor::Union{Nothing,MySQL.TextCursor} = nothing
    try
        textcursor = tx_context(conn) do
            return DBInterface.execute(conn, sql, mysql_store_result=false)
        end
        return Tables.dictrowtable(textcursor)
    finally
        DBInterface.close!(textcursor)
    end
end

function execute(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any}) -> Tables.dictrowtable
    sql = MySQL.escape(conn, sql)
    cursor::Union{Nothing,MySQL.Cursor} = nothing
    try
        cursor = tx_context(conn) do
            stmt = prepare(conn, sql)
            try
                return DBInterface.execute(stmt, params, mysql_store_result=false)
            finally
                DBInterface.close!(stmt)
            end
        end
        return Tables.dictrowtable(cursor)
    finally
        DBInterface.close!(cursor)
    end
end


function execute_query(conn::MySQL.Connection, sql::AbstractString; 
    limit:Union{Nothing,Int}=nothing) -> Tables.dictrowtable

    sql = MySQL.escape(conn, sql)
    if limit !== nothing
        if occursin("limit", sql)
            sql = strip(sql[1: findfirst("limit", sql).start-1])
        end
        sql = "$sql limit $limit"
    end
    textcursor::Union{Nothing,MySQL.TextCursor} = nothing
    try
        textcursor =  DBInterface.execute(conn, sql, mysql_store_result=false)
        return Tables.dictrowtable(textcursor)
    finally
        DBInterface.close!(textcursor)
    end
end

function execute_query(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any}; 
    limit:Union{Nothing,Int}=nothing) -> Tables.dictrowtable

    sql = MySQL.escape(conn, sql)
    if limit !== nothing
        if occursin("limit", sql)
            sql = strip(sql[1: findfirst("limit", sql).start-1])
        end
        sql = "$sql limit $limit"
    end
    cursor::Union{Nothing,MySQL.Cursor} = nothing
    try
        stmt = prepare(conn, sql)
        cursor = DBInterface.execute(stmt, params, mysql_store_result=false)
        DBInterface.close!(stmt)
        Tables
        return Tables.dictrowtable(cursor)
    finally
        DBInterface.close!(cursor)
    end
end

