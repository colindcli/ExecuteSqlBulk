using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    public static class SqlBulkExt
    {
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
    }
}
