using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace ExecuteSqlBulk
{
    public class QueryableBuilder
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
        /// <param name="whereConditions"></param>
        /// <returns></returns>
        protected internal static IQuery<T> GetListByBulk<T>(object whereConditions)
        {
            var name = GetName<T>();
            var where = GetWhere(whereConditions);

            return new Queryable<T>()
            {
                TableName = $"[{name}]",
                Where = where
            };
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

        protected internal static string GetPropertyName<T, TResult>(Expression<Func<T, TResult>> expression)
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
            else if (expression.Body is MemberExpression)
            {
                rtn = ((MemberExpression)expression.Body).Member.Name;
            }
            else if (expression.Body is ParameterExpression)
            {
                rtn = ((ParameterExpression)expression.Body).Type.Name;
            }
            return rtn;
        }
    }
}
