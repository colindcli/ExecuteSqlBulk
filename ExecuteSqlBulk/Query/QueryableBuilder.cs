using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace ExecuteSqlBulk
{
    internal class QueryableBuilder
    {
        private static Dictionary<Type, string> TypeNames { get; } = new Dictionary<Type, string>();
        private static Dictionary<object, string> ObjectWhere { get; } = new Dictionary<object, string>();

        private static string GetName<T>()
        {
            var type = typeof(T);
            var b = TypeNames.TryGetValue(type, out var name);
            if (b)
            {
                return name;
            }

            var value = type.Name;
            lock (TypeNames)
            {
                b = TypeNames.TryGetValue(type, out var _);
                if (!b)
                {
                    TypeNames.Add(type, value);
                }
            }
            return value;
        }

        /// <summary>
        /// 批量获取列表(依赖Dapper)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="whereConditions">eg: new { Id =1 } 或 new { Id = new []{1, 2}.ToList() }</param>
        /// <returns></returns>
        internal static IQuery<T> GetListByBulk<T>(object whereConditions)
        {
            var name = GetName<T>();
            var where = GetWhere(whereConditions);

            var res = new Queryable<T>()
            {
                TableName = $"[{name}]",
                Where = where
            };

            return res;
        }

        /// <summary>
        /// 批量获取列表(依赖Dapper)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="whereConditions">eg: new { Id =1 } 或 new { Id = new []{1, 2}.ToList() }</param>
        /// <param name="selectColumns">eg: p => p.Id 或 p => new { p.Id, p.Name }</param>
        /// <returns></returns>
        internal static IQuery<T> GetListByBulk<T>(object whereConditions, Expression<Func<T, object>> selectColumns) where T : new()
        {
            var name = GetName<T>();
            var where = GetWhere(whereConditions);

            var res = new Queryable<T>()
            {
                TableName = $"[{name}]",
                Where = where
            };

            //列
            var cols = selectColumns.GetColumns();
            if (cols != null && cols.Count > 0)
            {
                res.SelectColumns = string.Join(",", cols.Select(p => $"[{p}]"));
            }

            return res;
        }

        /// <summary>
        /// 批量获取列表(依赖Dapper)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="likeColumns">eg:p => new { p.Id, p.Name }</param>
        /// <param name="keywords">搜索关键词集合</param>
        /// <param name="param">返回参数</param>
        /// <returns></returns>
        internal static IQuery<T> GetListByBulkLike<T>(Func<T, object> likeColumns, List<string> keywords, out Dictionary<string, object> param) where T : new()
        {
            var name = GetName<T>();
            var where = GetWhere(likeColumns, keywords, out param);

            return new Queryable<T>()
            {
                TableName = $"[{name}]",
                Where = where
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="likeColumns"></param>
        /// <param name="keywords"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        internal static string GetWhere<T>(Func<T, object> likeColumns, List<string> keywords, out Dictionary<string, object> param) where T : new()
        {
            param = new Dictionary<string, object>();
            if (keywords.Count == 0)
            {
                return "";
            }

            var concat = GetQueryColumn(likeColumns);
            if (string.IsNullOrWhiteSpace(concat))
            {
                return "";
            }

            var sb = new StringBuilder();
            sb.Append(" WHERE");
            var list = new List<string>();
            for (var i = 0; i < keywords.Count; i++)
            {
                var name = $"Keyword__{i}";
                var value = keywords[i];
                param.Add(name, value);
                list.Add($" {concat} LIKE '%' + @{name} + '%'");
            }
            sb.Append(string.Join(" AND", list));
            return sb.ToString();
        }

        /// <summary>
        /// 获取拼接的查询列
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="likeColumns"></param>
        /// <returns></returns>
        internal static string GetQueryColumn<T>(Func<T, object> likeColumns) where T : new()
        {
            var columnObj = likeColumns.Invoke(new T());
            var sb = new StringBuilder();
            var fields = columnObj.GetType().GetProperties();
            if (fields.Length > 0)
            {
                var list = new List<string>();
                foreach (var field in fields)
                {
                    list.Add($"[{field.Name}]");
                }
                sb.Append(string.Join(" + ' ' + ", list));
            }
            return sb.ToString();
        }

        private static string GetWhere(object whereConditions)
        {
            if (whereConditions == null)
            {
                return "";
            }

            var b = ObjectWhere.TryGetValue(whereConditions, out var where);
            if (b)
            {
                return where;
            }

            var sb = new StringBuilder();
            var fields = whereConditions.GetType().GetProperties();
            if (fields.Length > 0)
            {
                sb.Append(" WHERE");
                var addAnd = false;
                foreach (var field in fields)
                {
                    if (addAnd)
                    {
                        sb.Append(" AND");
                    }
                    else
                    {
                        addAnd = true;
                    }

                    var fieldName = field.Name;
                    var fieldValue = field.GetValue(whereConditions);
                    switch (fieldValue)
                    {
                        case string _:
                            sb.Append($" [{fieldName}]=@{fieldName}");
                            break;
                        case IEnumerable _:
                            sb.Append($" [{fieldName}] IN @{fieldName}");
                            break;
                        default:
                            sb.Append($" [{fieldName}]=@{fieldName}");
                            break;
                    }
                }
            }

            var wh = sb.ToString();
            lock (ObjectWhere)
            {
                b = ObjectWhere.TryGetValue(whereConditions, out var _);
                if (!b)
                {
                    ObjectWhere.Add(whereConditions, wh);
                }
            }
            return wh;
        }

        internal static string GetPropertyName<T, TResult>(Expression<Func<T, TResult>> expression)
        {
            if (expression == null)
            {
                return "";
            }

            var rtn = "";
            if (expression.Body is UnaryExpression body)
            {
                rtn = ((MemberExpression)body.Operand).Member.Name;
            }
            else if (expression.Body is MemberExpression body2)
            {
                rtn = body2.Member.Name;
            }
            else if (expression.Body is ParameterExpression body3)
            {
                rtn = body3.Type.Name;
            }
            return rtn;
        }
    }
}
