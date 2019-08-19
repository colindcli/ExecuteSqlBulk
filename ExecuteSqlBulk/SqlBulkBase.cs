using System;
using System.Data;
using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    internal class SqlBulkBase : IDisposable
    {
        protected SqlConnection Connection { get; set; }
        protected SqlBulkCopy SqlBulkCopy { get; set; }

        protected void SqlBulk(SqlConnection connection)
        {
            Connection = connection;
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
            SqlBulkCopy = new SqlBulkCopy(connection);
        }

        public void Dispose()
        {
            SqlBulkCopy.Close();
            SqlBulkCopy = null;
        }
    }
}
