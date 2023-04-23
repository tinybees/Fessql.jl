# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

Base.@kwdef struct MysqlDBConfig <: DBConfig
    dbhost::String
    username::String
    password::String
    port::Int = 3306
    dbname::String
    reconnect::Bool = true
    autocommit::Bool = false
end

function initialize_db(conf::MysqlDBConfig, db_binds::Union{Nothing, Dict{String, MysqlDBConfig}} = nothing; kwargs...)
    try
        # 默认的库连接
        dbconns["default"] = DBInterface.connect(
            MySQL.Connection, conf.dbhost, conf.username, conf.password;
            db = conf.dbname, port = conf.port, reconnect = conf.reconnect, kwargs...)
        # 设置是否自动提交默认否
        MySQL.API.autocommit(dbconns["default"].mysql, conf.autocommit)
        # db_binds中的连接
        if db_binds !== nothing
            for (name, dbargs) in db_binds
                dbconns[name] = DBInterface.connect(
                    MySQL.Connection, dbargs.dbhost, dbargs.username, dbargs.password;
                    db = dbargs.dbname, port = dbargs.port, reconnect = dbargs.reconnect, kwargs...)
                # 设置是否自动提交默认否
                MySQL.API.autocommit(dbconns[name].mysql, dbargs.autocommit)
            end
        end
    catch ex
        @error ex
    end
    @info "初始化所有的数据库连接成功"
end

function initialize_db(conf::Dict{String, Any}, db_binds::Union{Nothing, Dict{String, Any}} = nothing; kwargs...)
    conf2 = MysqlDBConfig(conf...)
    db_binds2 = Dict(name => MysqlDBConfig(one_conf...) for (name, one_conf) in db_binds)
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

function tx_context(f::Function, conn::MySQL.Connection)
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

function _execute(conn::MySQL.Connection, sql::AbstractString)
    textcursor::Union{Nothing, MySQL.TextCursor} = nothing
    try
        textcursor = DBInterface.execute(conn, MySQL.escape(conn, sql); mysql_store_result = false)
        return Tables.dictrowtable(textcursor)
    finally
        if textcursor !== nothing
            DBInterface.close!(textcursor)
        end
    end
end

function _execute(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any})
    stmt::Union{Nothing, MySQL.Statement} = nothing
    cursor::Union{Nothing, MySQL.Cursor} = nothing
    try
        stmt = prepare(conn, MySQL.escape(conn, sql))
        cursor = DBInterface.execute(stmt, params; mysql_store_result = false)
        return Tables.dictrowtable(cursor)
    finally
        if stmt !== nothing
            DBInterface.close!(stmt)
        end
        if cursor !== nothing
            DBInterface.close!(cursor)
        end
    end
end

function execute!(conn::MySQL.Connection, sql::AbstractString)
    return tx_context(conn) do 
        _execute(conn, sql)
    end
end

function execute!(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any})
    return tx_context(conn) do 
        _execute(conn, sql, params)
    end
end

function execute_query(conn::MySQL.Connection, sql::AbstractString; limit:Union{Nothing, Int} = nothing)
    if limit !== nothing
        if occursin("limit", sql)
            sql = strip(sql[1:(findfirst("limit", sql).start - 1)])
        end
        sql = "$sql limit $limit"
    end
    return _execute(conn, sql)
end

function execute_query(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any};
    limit:Union{Nothing, Int} = nothing)
    if limit !== nothing
        if occursin("limit", sql)
            sql = strip(sql[1:(findfirst("limit", sql).start - 1)])
        end
        sql = "$sql limit $limit"
    end
    return _execute(conn, sql, params)
end

function delete!(conn::MySQL.Connection, model::Type{T}, q::String) where {T <: Model}
    del_sql::String = "delete from $(tablename(model)) where $q;"
    _execute(conn, del_sql)
end

function delete!(conn::MySQL.Connection, model::Type{T}, q::String, params::Vector{Any}) where {T <: Model}
    del_sql::String = "delete from $(tablename(model)) where $q;"
    _execute(conn, del_sql, params)
end

function get_talias(sql::String)::String
    result::Union{Nothing, RegexMatch} = match(r"AS (`.+`)", sql)
    if result !== nothing
        return result.captures[1]
    else
        return ""
    end
end

function delete!(conn::MySQL.Connection, q::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    raw_sql = q.raw
    # eg: delete t1 from xxxx as t1 where t1.id = 1
    del_sql = "delete $(get_talias(raw_sql)) $(raw_sql[findfirst("FROM", raw_sql).start: end])"
    if length(q.vars) > 0
        _execute(conn, del_sql, FunSQL.pack(q.vars, args_values))
    else
        _execute(conn, del_sql)
    end
end

function delete!(conn::MySQL.Connection, q::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    delete!(conn, FunSQL.render(q; tables = mtables, dialect = :mysql), args_values)
end
