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

function Base.getproperty(obj::T, name::Symbol) {T < Model}
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
