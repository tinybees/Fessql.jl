module Fessql

using MySQL, FunSQL, Tables
export Model, colnames, model2tuple, FunSQL, Column, tablename

"""
    用于描述表的抽象类型Model
"""
abstract type Model end

"""
用于数据库配置的抽象类型DBConfig
"""
abstract type DBConfig end

"""
    colnames(m::Type{T}) where {T<:Model}
    对于给定的Model返回列的数组.
"""
colnames(m::Type{T}) where {T <: Model} = [fieldname for fieldname in fieldnames(m) if fieldname != :__tablename__]

"""
    model2tuple(m::T) where {T<:Model}
    转换给定的model到元祖.
"""
function model2tuple(m::T) where {T <: Model}
    return tuple(map(col -> (col, getfield(m, col)), colnames(T))...)
end

"""
    tablename(m::T) where {T<:Model}
    返回给定Model的表名,如果没有指定则报错.
"""
function tablename(m::Type{T})::String where {T <: Model}
    if !(:__tablename__ in fieldnames(m))
        error("Struct Model $m 未定义__tablename__表名字段")
    end
    return m.__tablename__
end

# 存储数据库连接
const dbconns = Dict{String, DBInterface.Connection}()

"""
以下定义一些必要的抽象类型和函数
"""

"""
    initialize_db(conf::MysqlDBConfig, db_binds::Union{Nothing,Dict{String,MysqlDBConfig}}=nothing; kwargs...)
    initialize_db(conf::Dict{String,Any},db_binds::Union{Nothing,Dict{String,Any}}=nothing; kwargs...)
    初始化数据库链接，初始化配置文件中的所有数据库连接
"""
function initialize_db end

"""
    close_db()
    close_db(db::Any)
    关闭数据库链接.
"""
function close_db end

"""
    execute(db::Any, query::Any, params::Any)
    执行给定的查询用给定的参数，主要用于更新，删除和插入.
"""
function execute end

"""
    _execute(conn::MySQL.Connection, sql::AbstractString)
    _execute(conn::MySQL.Connection, sql::AbstractString, params::Vector{Any})
    主要用于无事务、有参数、参数基础执行方法.
"""
function _execute end

"""
    query_execute(db::Any, query::Any, params::Any)
    执行给定的查询用给定的参数，主要用于查询.
"""
function execute_query end

"""
    find_one(db::Any, m::Type{T}) where {T <: Model}
    查询单条数据用给定的查询和参数.
"""
function find_one end

"""
    find_all(db::Any, m::Type{T}) where {T <: Model}
    查询所有数据用给定的查询和参数.
"""
function find_all end

"""
    find_many(db::Any, m::Type{T}) where {T <: Model}
    分页查询数据用给定的查询和参数.
"""
function find_many end

"""
    find_many(db::Any, m::Type{T}) where {T <: Model}
    查询数量用给定的查询和参数.
"""
function find_count end

"""
    insert!(db::Any, model::T) where {T <: Model}
    插入单个Model数据或者批量model数据.
"""
function insert! end

"""
    update!(db::Any, model::T) where {T <: Model}
    更新一条或者多条Model数据.
"""
function update! end

"""
    delete!(db::Any, model::T) where {T <: Model}
    删除一行或者多行用给定的Model和给定的查询条件.
"""
function delete! end

"""
    function tx_context(f::Function, conn::MySQL.Connection)
    插入，更新和删除事务上下文.
"""
function tx_context end

include("query.jl")
include("backends/mysql.jl")

end # module Fessql
