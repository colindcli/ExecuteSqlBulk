using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IQuery<T>
    {
        SqlConnection Db { get; set; }
        SqlTransaction Transaction { get; set; }
        int? CommandTimeout { get; set; }
        object WhereConditions { get; set; }
        int Top { get; set; }
        string TableName { get; set; }
        string Where { get; set; }
        string OrderBy { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class Queryable<T> : IQuery<T>
    {
        public SqlConnection Db { get; set; }
        public SqlTransaction Transaction { get; set; }
        public int? CommandTimeout { get; set; }
        public object WhereConditions { get; set; }
        public int Top { get; set; } = -1;
        public string TableName { get; set; }
        public string Where { get; set; }
        public string OrderBy { get; set; }
    }
}
