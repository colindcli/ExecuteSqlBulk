using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace ExecuteSqlBulk
{
    internal class SqlBulkUpdate : SqlBulkBase
    {
        internal SqlBulkUpdate(SqlConnection connection)
        {
            SqlBulk(connection);
        }

        /// <summary>
        /// 批量更新数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="destinationTableName">表名</param>
        /// <param name="data">数据</param>
        /// <param name="pkColumns">主键</param>
        /// <param name="updateColumns">更新的列集合</param>
        internal int BulkUpdate<T>(string destinationTableName, IEnumerable<T> data, List<string> pkColumns, List<string> updateColumns)
        {
            var tempTablename = "##" + destinationTableName + "_" + Guid.NewGuid().ToString("N");
            //
            CreateTempTable(destinationTableName, tempTablename);
            //
            var dataAsArray = data as T[] ?? data.ToArray();
            SqlBulkCopy.DestinationTableName = tempTablename;
            var dt = Common.GetDataTableFromFields(dataAsArray, SqlBulkCopy);
            SqlBulkCopy.BatchSize = 100000;
            SqlBulkCopy.WriteToServer(dt);

            var row = MergeTempAndDestination(destinationTableName, tempTablename, pkColumns, updateColumns);
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

        private int MergeTempAndDestination(string destinationTableName, string tempTablename, List<string> pkColumns, List<string> updateColumns)
        {
            var pkSql = new StringBuilder();
            for (var i = 0; i < pkColumns.Count; i++)
            {
                if (i > 0)
                {
                    pkSql.Append(" AND");
                }
                pkSql.Append($" Target.[{pkColumns[i]}]=Source.[{pkColumns[i]}]");
            }

            var updateSql = new StringBuilder();
            for (var i = 0; i < updateColumns.Count; i++)
            {
                if (i > 0)
                {
                    updateSql.Append(",");
                }
                updateSql.Append($"Target.[{updateColumns[i]}]=Source.[{updateColumns[i]}]");
            }

            var mergeSql = $@"
                        UPDATE Target
                            SET {updateSql}
                        FROM [{destinationTableName}] AS Target
                        INNER JOIN [{tempTablename}] AS Source ON {pkSql} ;

                        -- INSERT [{destinationTableName}]  
                        --     SELECT * FROM [{tempTablename}] AS Source
                        -- WHERE NOT EXISTS (SELECT 1 FROM [{destinationTableName}] WHERE {pkSql});
";

            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = mergeSql;
            return cmdTempTable.ExecuteNonQuery();
        }

        private void CreateTempTable(string destinationTableName, string tempTablename)
        {
            var cmdTempTable = Connection.CreateCommand();
            cmdTempTable.CommandText = $"SELECT TOP 0 * INTO [{tempTablename}] FROM [{destinationTableName}]";
            cmdTempTable.ExecuteNonQuery();
        }
    }
}