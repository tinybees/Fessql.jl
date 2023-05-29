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
    pool_size::Int = 10
    pool_recycle::Int = 1800
end

function initialize_db(conf::MysqlDBConfig, db_binds::Union{Nothing, Dict{String, MysqlDBConfig}} = nothing; kwargs...)
    try
        # 默认的库连接
        dbconns["default"] = ConnectionPod{ConnectionManager}(Channel(conf.pool_size), 1, conf.pool_size, "default")
        create_poolconn(dbconns["default"], conf; kwargs)
        # db_binds中的连接
        if db_binds !== nothing
            for (name, dbargs) in db_binds
                dbconns[name] = ConnectionPod{ConnectionManager}(Channel(dbargs.pool_size), 1, dbargs.pool_size, name)
                create_poolconn(dbconns[name], dbargs; kwargs)
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
        for (_, pod) in dbconns
            for conn in pod.conns
                DBInterface.close!(conn.conn)
            end
            pod.numactive = 0 #  全部关闭后活跃数量为0
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
        return Tables.rowtable(textcursor)
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
        return Tables.rowtable(cursor)
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
        sql = "$sql limit $(limit)"
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

function insert!(conn::MySQL.Connection, insert_data::T) where {T <: Model}
    fields = map(colpairs -> colpairs[1], model2tuple(insert_data))
    values = map(colpairs -> colpairs[2], model2tuple(insert_data))

    insert_sql = "INSERT INTO $(tablename(
        typeof(insert_data))) ($(join(fields, ", "))) VALUES ($(join(fill("?", length(fields)), ", ")))"

    return _execute(conn, insert_sql, values)
end

function insert!(conn::MySQL.Connection, insert_data::Vector{T}) where {T <: Model}
    fields = map(colpairs -> colpairs[1], model2tuple(insert_data[0]))
    # values = [map(colpairs -> colpairs[2], model2tuple(one_model)) for one_model in model]
    insert_data_::Vector{String} = []
    for one_model in model
        row_data::Vector{String} = []
        for one_field in map(colpairs -> colpairs[2], model2tuple(one_model))
            if typeof(one_field) == String
                push!(row_data, "'$(one_field)'")
            elseif typeof(one_field) == Nothing
                push!(row_data, "NULL")
            elseif typeof(one_field) in (Date, DateTime)
                push!(row_data, date2string(one_field))
            else
                push!(row_data, one_field)
            end
        end
        push!(insert_data_, "($(join(row_data, ",")))")
    end

    insert_sql = "INSERT INTO $(tablename(typeof(model))) ($(join(fields, ", "))) VALUES ($(join(insert_data_)))"

    return _execute(conn, insert_sql)
end

function get_update_fields_str(update_data::Dict{String, Any})::String
    update_fields::Vector{String} = []
    for (field, value) in update_data
        if typeof(value) == String
            push!(update_fields, "$(field) = '$(value)'")
        elseif typeof(value) == Nothing
            push!(update_fields, "$(field) = NULL")
        elseif typeof(value) in (Date, DateTime)
            push!(update_fields, "$(field) = '$(date2string(value))'")
        else
            push!(update_fields, value)
        end
    end
    return join(update_fields, ",")
end

function update!(conn::MySQL.Connection, model::Type{T}, update_data::Dict{String, Any}, q::String) where {T <: Model}
    update_sql::String = "update $(tablename(model)) set $(get_update_fields_str(update_data)) where $q;"
    _execute(conn, update_sql)
end

function update!(conn::MySQL.Connection, model::Type{T}, update_data::Dict{String, Any},
    q::String, params::Vector{Any}) where {T <: Model}
    update_sql::String = "update $(tablename(model)) set $(get_update_fields_str(update_data)) where $q;"
    _execute(conn, update_sql, params)
end

function update!(conn::MySQL.Connection, update_data::Dict{String, Any}, q::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    raw_sql = q.raw
    # eg: update tablename as t1 set x=x where t1.id = 1
    tablename = raw_sql[(findfirst("FROM", sql).stop + 1):(findfirst("WHERE", raw_sql).start - 1)]
    where_part = raw_sql[(findfirst("WHERE", raw_sql).start):end]
    update_sql = "update $(tablename) set $(get_update_fields_str(update_data)) $(where_part);"
    if length(q.vars) > 0
        _execute(conn, update_sql, FunSQL.pack(q.vars, args_values))
    else
        _execute(conn, update_sql)
    end
end

function update!(conn::MySQL.Connection, update_data::Dict{String, Any}, q::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    update!(conn, update_data, FunSQL.render(q; tables = mtables, dialect = :mysql), args_values)
end

function find_count(conn::MySQL.Connection, sql::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)::Int
    raw_sql = sql.raw
    if length(sql.vars) > 0
        result = _execute(conn, raw_sql, FunSQL.pack(sql.vars, args_values))
    else
        result = _execute(conn, raw_sql)
    end
    return result[1][:count]
end

function find_count(conn::MySQL.Connection, sql::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)::Int
    return find_count(conn, FunSQL.render(sql; tables = mtables, dialect = :mysql), args_values)
end

function find_one(conn::MySQL.Connection, sql::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    raw_sql = sql.raw
    if length(sql.vars) > 0
        result = _execute(conn, raw_sql, FunSQL.pack(sql.vars, args_values))
    else
        result = _execute(conn, raw_sql)
    end
    return result[1]
end

function find_one(conn::MySQL.Connection, sql::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    return find_one(conn, FunSQL.render(sql; tables = mtables, dialect = :mysql), args_values)
end

function find_all(conn::MySQL.Connection, sql::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    raw_sql = sql.raw
    if length(sql.vars) > 0
        return _execute(conn, raw_sql, FunSQL.pack(sql.vars, args_values))
    else
        return _execute(conn, raw_sql)
    end
end

function find_all(conn::MySQL.Connection, sql::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing)
    return find_all(conn, FunSQL.render(sql; tables = mtables, dialect = :mysql), args_values)
end

function find_many(conn::MySQL.Connection, sql::FunSQL.SQLString,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing; page::Int = 1, per_page::Int = 20)::FesPagination
    raw_sql = sql.raw
    if occursin("limit", raw_sql)
        raw_sql = strip(raw_sql[1:(findfirst("limit", raw_sql).start - 1)])
    end
    paginate_sql = "$raw_sql limit $((page-1)*per_page), $(per_page)"
    count_sql = "select count(*) as tcount $(raw_sql[(findfirst("FROM", raw_sql).start):end])"
    if length(sql.vars) > 0
        total_data = _execute(conn, count_sql, FunSQL.pack(sql.vars, args_values))
        items = _execute(conn, paginate_sql, FunSQL.pack(sql.vars, args_values))
    else
        total_data = _execute(conn, count_sql)
        items = _execute(conn, paginate_sql)
    end
    total_count = total_data[0][:tcount]

    return FesPagination(; sql = sql, args_values = args_values, page = page, per_page = per_page,
        total = total_count, items = items)
end

function find_many(conn::MySQL.Connection, sql::FunSQL.SQLNode,
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing; page::Int = 1, per_page::Int = 20)::FesPagination
    return find_many(conn, FunSQL.render(sql; tables = mtables, dialect = :mysql), args_values;
        page = page, per_page = per_page)
end