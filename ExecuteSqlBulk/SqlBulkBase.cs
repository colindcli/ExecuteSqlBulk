using System;
using System.Data;
using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    internal class SqlBulkBase : IDisposable
    {
        protected SqlConnection Connection { get; set; }
        protected SqlBulkCopy SqlBulkCopy { get; set; }

        /// <summary>
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tran"></param>
        /// <param name="option"></param>
        protected void SqlBulk(SqlConnection connection, SqlTransaction tran, SqlBulkCopyOptions option)
        {
            Connection = connection;
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
            SqlBulkCopy = new SqlBulkCopy(connection, option, tran);
        }

        public void Dispose()
        {
            SqlBulkCopy.Close();
            SqlBulkCopy = null;
        }
    }
}
