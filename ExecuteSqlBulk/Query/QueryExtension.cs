using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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
        /// 获取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static IQuery<T> GetListByBulk<T>(this SqlConnection db, object whereConditions, SqlTransaction transaction = null, int? commandTimeout = null)
        {
            var obj = QueryableBuilder.GetListByBulk<T>(whereConditions);
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
            var sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} * FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
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
            var sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} * FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
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
    }
}
