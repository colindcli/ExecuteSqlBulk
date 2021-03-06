﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// 扩展
    /// </summary>
    public static class SqlBulkExt
    {
        /// <summary>
        /// 批量插入数据（支持NotMapped属性）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="tran"></param>
        public static void BulkInsert<T>(this SqlConnection db, List<T> dt, SqlTransaction tran = null)
        {
            var tableName = typeof(T).Name;
            BulkInsert(db, tableName, dt, tran);
        }

        /// <summary>
        /// Bulk Insert with a given destination name
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="dt"></param>
        /// <param name="tran"></param>
        public static void BulkInsert<T>(this SqlConnection db, string tableName, List<T> dt, SqlTransaction tran = null)
        {
            using (var sbc = new SqlBulkInsert(db, tran))
            {
                sbc.BulkInsert(tableName, dt);
            }
        }

        /// <summary>
        /// 批量更新数据（支持NotMapped属性）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TUpdateColumn"></typeparam>
        /// <typeparam name="TPkColumn"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnUpdateExpression">更新列集合</param>
        /// <param name="columnPrimaryKeyExpression">主键列</param>
        /// <param name="tran"></param>
        /// <returns>受影响行</returns>
        public static int BulkUpdate<T, TUpdateColumn, TPkColumn>(this SqlConnection db, List<T> dt, Expression<Func<T, TUpdateColumn>> columnUpdateExpression, Expression<Func<T, TPkColumn>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            var tableName = typeof(T).Name;
            return BulkUpdate(db, tableName, dt, columnUpdateExpression, columnPrimaryKeyExpression, tran);
        }


        /// <summary>
        /// 批量更新数据（支持NotMapped属性）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TUpdateColumn"></typeparam>
        /// <typeparam name="TPkColumn"></typeparam>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="dt"></param>
        /// <param name="columnUpdateExpression">更新列集合</param>
        /// <param name="columnPrimaryKeyExpression">主键列</param>
        /// <param name="tran"></param>
        /// <returns>受影响行</returns>
        public static int BulkUpdate<T, TUpdateColumn, TPkColumn>(this SqlConnection db, string tableName, List<T> dt, Expression<Func<T, TUpdateColumn>> columnUpdateExpression, Expression<Func<T, TPkColumn>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression不能为空");
            }
            if (columnUpdateExpression == null)
            {
                throw new Exception("columnInputExpression不能为空");
            }

            var pkColumns = GetColumns(columnPrimaryKeyExpression);
            if (pkColumns.Count == 0)
            {
                throw new Exception("主键不能为空");
            }

            var updateColumns = GetColumns(columnUpdateExpression);
            if (updateColumns.Count == 0)
            {
                throw new Exception("更新列不能为空");
            }

            using (var sbu = new SqlBulkUpdate(db, tran))
            {
                return sbu.BulkUpdate(tableName, dt, pkColumns, updateColumns);
            }
        }

        /// <summary>
        /// 获取列
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TColumn"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        internal static List<string> GetColumns<T, TColumn>(this Expression<Func<T, TColumn>> expression) where T : new()
        {
            if (expression.Body is MemberExpression memberBody)
            {
                return new List<string>() { memberBody.Member.Name };
            }

            if (expression.Body is UnaryExpression unaryBody)
            {
                var name = ((MemberExpression)unaryBody.Operand).Member.Name;
                return new List<string>() { name };
            }

            if (expression.Body is ParameterExpression parameterBody)
            {
                return new List<string>() { parameterBody.Type.Name };
            }

            var t = new T();
            var obj = expression.Compile().Invoke(t);
            var cols = Common.GetColumns(obj);
            return cols.ToList();
        }

        /// <summary>
        /// 批量删除数据（支持NotMapped属性）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TPk"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnPrimaryKeyExpression"></param>
        /// <param name="tran"></param>
        /// <returns>受影响行</returns>
        public static int BulkDelete<T, TPk>(this SqlConnection db, List<T> dt, Expression<Func<T, TPk>> columnPrimaryKeyExpression, SqlTransaction tran = null) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression不能为空");
            }

            var pkColumns = GetColumns(columnPrimaryKeyExpression);
            if (pkColumns.Count == 0)
            {
                throw new Exception("主键不能为空");
            }

            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkDelete(db, tran))
            {
                return sbc.BulkDelete(tableName, dt, pkColumns);
            }
        }

        /// <summary>
        /// 清空数据所有数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="tran"></param>
        public static void BulkDelete<T>(this SqlConnection db, SqlTransaction tran = null)
        {
            var tableName = typeof(T).Name;
            BulkDelete(db, tableName, tran);
        }

        /// <summary>
        /// Bulk Delete with given table name
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tableName"></param>
        /// <param name="tran"></param>
        public static void BulkDelete(this SqlConnection db, string tableName, SqlTransaction tran = null)
        {
            using (var sbc = new SqlBulkDelete(db, tran))
            {
                sbc.BulkDelete(tableName);
            }
        }
    }
}
