using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// 
    /// </summary>
    public static class QueryExtension
    {
        /// <summary>
        /// 删除数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        public static int DeleteListByBulk<T>(this IDbConnection db, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null) where T : new()
        {
            var obj = QueryableBuilder.GetListByBulk<T>(whereConditions);
            var sql = $"DELETE FROM {obj.TableName}{obj.Where};";
            return db.Execute(sql, whereConditions, transaction, commandTimeout, CommandType.Text);
        }

        /// <summary>
        /// 基于字段匹配集合查询数据（获取首条FirstOrDefault，排序OrderBy）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static IQuery<T> GetListByBulk<T>(this IDbConnection db, object whereConditions, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var obj = QueryableBuilder.GetListByBulk<T>(whereConditions);
            obj.Db = db;
            obj.Transaction = transaction;
            obj.CommandTimeout = commandTimeout;
            obj.WhereConditions = whereConditions;
            return obj;
        }

        /// <summary>
        /// 基于字段匹配集合查询数据（获取首条FirstOrDefault，排序OrderBy）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="whereConditions"></param>
        /// <param name="selectColumns"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static IQuery<T> GetListByBulk<T>(this IDbConnection db, object whereConditions, Expression<Func<T, object>> selectColumns, IDbTransaction transaction = null, int? commandTimeout = null) where T : new()
        {
            var obj = QueryableBuilder.GetListByBulk(whereConditions, selectColumns);
            obj.Db = db;
            obj.Transaction = transaction;
            obj.CommandTimeout = commandTimeout;
            obj.WhereConditions = whereConditions;
            return obj;
        }

        /// <summary>
        /// 基于like关键词查询数据 （获取首条FirstOrDefault，排序OrderBy）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="likeColumns">like查询的列。eg:p=>new { p.Name, p.Text }</param>
        /// <param name="keywords">关键词集合</param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static IQuery<T> GetListByBulkLike<T>(this IDbConnection db, Func<T, object> likeColumns, List<string> keywords, IDbTransaction transaction = null, int? commandTimeout = null) where T : new()
        {
            var obj = QueryableBuilder.GetListByBulkLike(likeColumns, keywords, out var whereConditions);
            obj.Db = db;
            obj.Transaction = transaction;
            obj.CommandTimeout = commandTimeout;
            obj.WhereConditions = whereConditions;
            return obj;
        }

        /// <summary>
        /// 取列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static List<T> ToList<T>(this IQuery<T> obj)
        {
            var col = string.IsNullOrWhiteSpace(obj.SelectColumns) ? "*" : obj.SelectColumns;
            var sql = string.Empty;
            if (QueryConfig.DialectServer == Dialect.SqlServer)
            {
                sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
            }
            else if (QueryConfig.DialectServer == Dialect.MySql)
            {
                sql = obj.Top > 0
                    ? $"SELECT {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy} LIMIT 0,{obj.Top};"
                    : $"SELECT {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
            }

            return obj.Db.Query<T>(sql, obj.WhereConditions, transaction: obj.Transaction, commandTimeout: obj.CommandTimeout, commandType: CommandType.Text).ToList();
        }

        /// <summary>
        /// 取第一条
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static T FirstOrDefault<T>(this IQuery<T> obj)
        {
            obj.Top = 1;
            var col = string.IsNullOrWhiteSpace(obj.SelectColumns) ? "*" : obj.SelectColumns;
            var sql = string.Empty;
            if (QueryConfig.DialectServer == Dialect.SqlServer)
            {
                sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
            }
            else if (QueryConfig.DialectServer == Dialect.MySql)
            {
                if (obj.Top > 0)
                {
                    sql = $"SELECT {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy} LIMIT 0,{obj.Top};";
                }
                else
                {
                    sql = $"SELECT {col} FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
                }
            }
            return obj.Db.Query<T>(sql, obj.WhereConditions, transaction: obj.Transaction, commandTimeout: obj.CommandTimeout, commandType: CommandType.Text).FirstOrDefault();
        }

        /// <summary>
        /// 获取几条
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="number"></param>
        /// <returns></returns>
        public static IQuery<T> Take<T>(this IQuery<T> obj, int number)
        {
            obj.Top = number;
            return obj;
        }

        /// <summary>
        /// 顺序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="obj"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IOrderQuery<T> OrderBy<T, TResult>(this IQuery<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"ORDER BY {QueryableBuilder.GetPropertyName(predicate)} ASC";
            return (IOrderQuery<T>)obj;
        }

        /// <summary>
        /// 顺序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="obj"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IOrderQuery<T> ThenBy<T, TResult>(this IOrderQuery<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"{obj.OrderBy},{QueryableBuilder.GetPropertyName(predicate)} ASC";
            return obj;
        }

        /// <summary>
        /// 倒序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="obj"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IOrderQuery<T> OrderByDescending<T, TResult>(this IQuery<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"ORDER BY {QueryableBuilder.GetPropertyName(predicate)} DESC";
            return (IOrderQuery<T>)obj;
        }

        /// <summary>
        /// 倒序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="obj"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IOrderQuery<T> ThenByDescending<T, TResult>(this IOrderQuery<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"{obj.OrderBy},{QueryableBuilder.GetPropertyName(predicate)} DESC";
            return obj;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        internal static string Ns(this string name)
        {
            if (QueryConfig.DialectServer == Dialect.SqlServer)
            {
                return $"[{name}]";
            }

            if (QueryConfig.DialectServer == Dialect.MySql)
            {
                return $"`{name}`";
            }

            return name;
        }
    }
}
