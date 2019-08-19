using System.Collections.Generic;
using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    internal class SqlBulkInsert : SqlBulkBase
    {
        internal SqlBulkInsert(SqlConnection connection)
        {
            SqlBulk(connection);
        }

        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName"></param>
        /// <param name="data"></param>
        internal void BulkInsert<T>(string destinationTableName, IEnumerable<T> data)
        {
            SqlBulkCopy.DestinationTableName = $"[{destinationTableName}]";
            var dt = Common.GetDataTableFromFields(data, SqlBulkCopy);

            SqlBulkCopy.BatchSize = 100000;
            SqlBulkCopy.WriteToServer(dt);
        }
    }
}