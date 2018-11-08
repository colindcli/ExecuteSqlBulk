using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ExecuteSqlBulk
{
    public class SqlBulkUpdate : SqlBulkBase
    {
        public SqlBulkUpdate(SqlConnection connection)
        {
            SqlBulk(connection);
        }

        /// <summary>
        /// 批量更新数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName">表名</param>
        /// <param name="data">数据</param>
        /// <param name="columnNameToMatch">主键</param>
        /// <param name="columnNamesToUpdate">更新的列集合</param>
        public int BulkUpdate<T>(string destinationTableName, IEnumerable<T> data, string columnNameToMatch, string[] columnNamesToUpdate)
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
            var row = MergeTempAndDestination(destinationTableName, tempTablename, columnNameToMatch, columnNamesToUpdate);
            //
            DropTempTable(tempTablename);

            return row;
        }

        private void DropTempTable(string tempTablename)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = "DROP TABLE " + tempTablename;
            cmdTempTable.ExecuteNonQuery();
        }

        private int MergeTempAndDestination(string destinationTableName, string tempTablename, string matchingColumn, string[] columnNamesToUpdate)
        {
            var updateSql = "";
            for (var i = 0; i < columnNamesToUpdate.Length; i++)
            {
                updateSql += string.Format("Target.[{0}]=Source.[{0}]", columnNamesToUpdate[i]);
                if (i < columnNamesToUpdate.Length - 1)
                {
                    updateSql += ",";
                }
            }
            var mergeSql = "MERGE INTO " + destinationTableName + " AS Target\r\n" +
                           "USING " + tempTablename + " AS Source\r\n" +
                           "ON\r\n" +
                           "Target." + matchingColumn + " = Source." + matchingColumn + "\r\n" +
                           "WHEN MATCHED THEN\r\n" +
                           "UPDATE SET " + updateSql + ";";

            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = mergeSql;
            return cmdTempTable.ExecuteNonQuery();
        }

        private void CreateTempTable(string destinationTableName, string tempTablename)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = "SELECT TOP 0 * \r\n" +
                                       "INTO " + tempTablename + "\r\n" +
                                       "FROM " + destinationTableName;
            cmdTempTable.ExecuteNonQuery();
        }
    }
}