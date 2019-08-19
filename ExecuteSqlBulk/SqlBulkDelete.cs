using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ExecuteSqlBulk
{
    internal class SqlBulkDelete : SqlBulkBase
    {
        internal SqlBulkDelete(SqlConnection connection)
        {
            SqlBulk(connection);
        }

        /// <summary>
        /// 删除数据表所有数据
        /// </summary>
        /// <param name="destinationTableName"></param>
        internal void BulkDelete(string destinationTableName)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = $"TRUNCATE TABLE [{destinationTableName}];";
            cmdTempTable.ExecuteNonQuery();
        }

        /// <summary>
        /// 批量删除数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName">表名</param>
        /// <param name="data">数据</param>
        /// <param name="columnNameToMatchs">主键</param>
        internal int BulkDelete<T>(string destinationTableName, IEnumerable<T> data, List<string> columnNameToMatchs)
        {
            var tempTablename = "#" + destinationTableName + "_" + Guid.NewGuid().ToString("N");
            //
            CreateTempTable(destinationTableName, tempTablename);
            //
            var dataAsArray = data as T[] ?? data.ToArray();
            SqlBulkCopy.DestinationTableName = tempTablename;
            var dt = Common.GetDataTableFromFields(dataAsArray, SqlBulkCopy);
            SqlBulkCopy.BatchSize = 100000;
            SqlBulkCopy.WriteToServer(dt);
            //
            var row = DeleteTempAndDestination(destinationTableName, tempTablename, columnNameToMatchs);
            //
            DropTempTable(tempTablename);

            return row;
        }

        private void DropTempTable(string tempTablename)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = $"DROP TABLE [{tempTablename}]";
            cmdTempTable.ExecuteNonQuery();
        }

        private int DeleteTempAndDestination(string destinationTableName, string tempTablename,
            List<string> columnNameToMatchs)
        {
            var sb = new StringBuilder(columnNameToMatchs.Count > 0 ? " WHERE" : "");
            for (var i = 0; i < columnNameToMatchs.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(" AND");
                }

                sb.Append($" t1.[{columnNameToMatchs[i]}]=t2.[{columnNameToMatchs[i]}]");
            }
            var deleteSql = $"DELETE [{destinationTableName}] FROM [{destinationTableName}] t1,{tempTablename} t2{sb};";

            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = deleteSql;
            return cmdTempTable.ExecuteNonQuery();
        }

        private void CreateTempTable(string destinationTableName, string tempTablename)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = $"SELECT TOP 0 * INTO [{tempTablename}] FROM [{destinationTableName}];";
            cmdTempTable.ExecuteNonQuery();
        }
    }
}