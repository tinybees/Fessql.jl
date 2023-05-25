# Copyright (c) 2023 guoyanfeng
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

using Dates, ULID

function date2string(value::DateTime)::String
    return Dates.format(value, "yyyy-mm-ddTHH:MM:SS.s")
end

function date2string(value::Date)::String
    return Dates.format(value, "yyyy-mm-dd")
end

"""
lulid()

生成一个小写的ulid字符串
"""
lulid() = lowercase(ulid())