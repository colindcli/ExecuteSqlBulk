using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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
        public static void BulkInsert<T>(this SqlConnection db, List<T> dt)
        {
            var tableName = typeof(T).Name;
            using (var sbc = new SqlBulkInsert(db))
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
        /// <returns>受影响行</returns>
        public static int BulkUpdate<T, TUpdateColumn, TPkColumn>(this SqlConnection db, List<T> dt, Expression<Func<T, TUpdateColumn>> columnUpdateExpression, Expression<Func<T, TPkColumn>> columnPrimaryKeyExpression) where T : new()
        {
            if (columnPrimaryKeyExpression == null)
            {
                throw new Exception("columnPrimaryKeyExpression不能为空");
            }
            if (columnUpdateExpression == null)
            {
                throw new Exception("columnInputExpression不能为空");
            }

            var tableName = typeof(T).Name;

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

            using (var sbu = new SqlBulkUpdate(db))
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
        private static List<string> GetColumns<T, TColumn>(Expression<Func<T, TColumn>> expression) where T : new()
        {
            var columns = new List<string>();
            if (expression.Body is MemberExpression body)
            {
                var col = body.Member.Name;
                columns.Add(col);
            }
            else
            {
                var t = new T();
                var obj = expression.Compile().Invoke(t);
                var cols = Common.GetColumns(obj);
                columns.AddRange(cols);
            }

            return columns;
        }

        /// <summary>
        /// 批量删除数据（支持NotMapped属性）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TPk"></typeparam>
        /// <param name="db"></param>
        /// <param name="dt"></param>
        /// <param name="columnPrimaryKeyExpression"></param>
        /// <returns>受影响行</returns>
        public static int BulkDelete<T, TPk>(this SqlConnection db, List<T> dt, Expression<Func<T, TPk>> columnPrimaryKeyExpression) where T : new()
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
            using (var sbc = new SqlBulkDelete(db))
            {
                return sbc.BulkDelete(tableName, dt, pkColumns);
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
    }
}
