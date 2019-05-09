using Dapper;
using ExecuteSqlBulk.SimpleSelect;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace ExecuteSqlBulk
{
    public static class SqlBulkExt
    {
        #region 批量更新

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        public static void BulkInsert<T>(this SqlConnection db, List<T> dt)
        {
            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkInsert(db))
            {
                sbc.BulkInsert(tableName, dt);
            }
        }

        /// <summary>
        /// 批量更新数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnUpdateExpression">更新列集合</param>
        /// <param name="columnPrimaryKeyExpression">主键列</param>
        /// <returns>受影响行</returns>
        public static int BulkUpdate<T>(this SqlConnection db, List<T> dt, Func<T, object> columnUpdateExpression, Func<T, object> columnPrimaryKeyExpression) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression不能为空");
            }
            if (columnUpdateExpression == null)
            {
                throw new Exception("columnInputExpression不能为空");
            }

            var t = new T();
            var tableName = typeof(T).Name;
            var pkObj = columnPrimaryKeyExpression.Invoke(t);
            var pks = Common.GetColumns(pkObj);
            if (pks.Length == 0)
            {
                throw new Exception("主键不能为空");
            }
            var obj = columnUpdateExpression.Invoke(t);
            var columns = Common.GetColumns(obj);
            if (columns.Length == 0)
            {
                throw new Exception("更新列不能为空");
            }

            using (var sbu = new SqlBulkUpdate(db))
            {
                return sbu.BulkUpdate(tableName, dt, pks[0], columns);
            }
        }

        /// <summary>
        /// 批量删除数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnPrimaryKeyExpression"></param>
        /// <returns>受影响行</returns>
        public static int BulkDelete<T>(this SqlConnection db, List<T> dt, Func<T, object> columnPrimaryKeyExpression) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression不能为空");
            }

            var t = new T();
            var pkObj = columnPrimaryKeyExpression.Invoke(t);
            var pks = Common.GetColumns(pkObj);
            if (pks.Length == 0)
            {
                throw new Exception("主键不能为空");
            }

            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkDelete(db))
            {
                return sbc.BulkDelete(tableName, dt, pks[0]);
            }
        }

        /// <summary>
        /// 清空数据所有数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        public static void BulkDelete<T>(this SqlConnection db)
        {
            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkDelete(db))
            {
                sbc.BulkDelete(tableName);
            }
        }

        #endregion

        #region GetListByBulk

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="whereConditions"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static SimpleSelectModel<T> GetListByBulk<T>(this SqlConnection db, object whereConditions, SqlTransaction transaction = null, int? commandTimeout = null)
        {
            var obj = SimpleSelectHelper.GetListByBulk<T>(whereConditions);
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
        public static List<T> ToList<T>(this SimpleSelectModel<T> obj)
        {
            var sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} * FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
            return obj.Db.Query<T>(sql, obj.WhereConditions, transaction: obj.Transaction, commandTimeout: obj.CommandTimeout).ToList();
        }

        /// <summary>
        /// 取第一条
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static T FirstOrDefault<T>(this SimpleSelectModel<T> obj)
        {
            obj.Top = 1;
            var sql = $"SELECT{(obj.Top >= 0 ? $" TOP ({obj.Top})" : "")} * FROM {obj.TableName} {obj.Where} {obj.OrderBy};";
            return obj.Db.Query<T>(sql, obj.WhereConditions, transaction: obj.Transaction, commandTimeout: obj.CommandTimeout).FirstOrDefault();
        }

        /// <summary>
        /// 获取几条
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="number"></param>
        /// <returns></returns>
        public static SimpleSelectModel<T> Take<T>(this SimpleSelectModel<T> obj, int number)
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
        public static SimpleSelectModel<T> OrderBy<T, TResult>(this SimpleSelectModel<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"ORDER BY {SimpleSelectHelper.GetPropertyName(predicate)} ASC";
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
        public static SimpleSelectModel<T> ThenBy<T, TResult>(this SimpleSelectModel<T> obj, Expression<Func<T, TResult>> predicate)
        {
            if (string.IsNullOrWhiteSpace(obj.OrderBy))
            {
                throw new Exception("请先调用OrderBy");
            }
            obj.OrderBy = $"{obj.OrderBy},{SimpleSelectHelper.GetPropertyName(predicate)} ASC";
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
        public static SimpleSelectModel<T> OrderByDescending<T, TResult>(this SimpleSelectModel<T> obj, Expression<Func<T, TResult>> predicate)
        {
            obj.OrderBy = $"ORDER BY {SimpleSelectHelper.GetPropertyName(predicate)} DESC";
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
        public static SimpleSelectModel<T> ThenByDescending<T, TResult>(this SimpleSelectModel<T> obj, Expression<Func<T, TResult>> predicate)
        {
            if (string.IsNullOrWhiteSpace(obj.OrderBy))
            {
                throw new Exception("请先调用OrderBy");
            }
            obj.OrderBy = $"{obj.OrderBy},{SimpleSelectHelper.GetPropertyName(predicate)} DESC";
            return obj;
        }

        #endregion
    }
}
