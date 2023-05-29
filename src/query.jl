# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

"""
    存储所有数据表的SQLtable用于SQL渲染
"""
const mtables = Dict{Symbol, SQLTable}()

"""
    用于描述表中的列结构
"""
Base.@kwdef struct Column
    name::Symbol
    doc::String = ""
end

function Base.getproperty(obj::T, name::Symbol) where {T <: Model}
    field_value = getfield(obj, name)
    if typeof(field_value) == Column
        return FunSQL.Get(field_value.name)
    else
        return field_value
    end
end

function FunSQL.From(m::Type{T}) where {T <: Model}
    symbol_tablename = Symbol(tablename(m))
    if !(symbol_tablename in mtables)
        mtables[symbol_tablename] = FunSQL.SQLTable(symbol_tablename; columns = [Symbol(val) for val in colnames(m)])
    end
    return FunSQL.From(symbol_tablename)
end

"""
    用于分页查询后的结果
"""
Base.@kwdef struct FesPagination
    sql::FunSQL.SQLString
    args_values::Union{Dict{Symbol, Any}, NamedTuple} = nothing
    page::Int = 1
    per_page::Int = 20
    total::Int
    items::Vector{NamedTuple}
end

"""The total number of pages"""
function fespages(fp::FesPagination)::Int
    if fp.per_page == 0 || fp.total == 0
        pages = 0
    else
        pages = Int(ceil(fp.total / fp.per_page))
    end
    return pages
end

"""True if a next page exists."""
has_next(fp::FesPagination) = fp.page < fespages(fp)

"""True if a previous page exists"""
has_prev(fp::FesPagination) = fp.page > 1

function prev_many(fp::FesPagination, conn::MySQL.Connection)::FesPagination
    return find_many(conn, fp.sql, fp.args_values; page = fp.page - 1, per_page = fp.per_page)
end

function next_many(fp::FesPagination, conn::MySQL.Connection)::FesPagination
    return find_many(conn, fp.sql, fp.args_values; page = fp.page + 1, per_page = fp.per_page)
end
