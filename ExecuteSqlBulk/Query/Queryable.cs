using System.Data.SqlClient;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IQuery<T>
    {
        /// <summary>
        /// 
        /// </summary>
        SqlConnection Db { get; set; }
        /// <summary>
        /// 
        /// </summary>
        SqlTransaction Transaction { get; set; }
        /// <summary>
        /// 
        /// </summary>
        int? CommandTimeout { get; set; }
        /// <summary>
        /// 
        /// </summary>
        object WhereConditions { get; set; }
        /// <summary>
        /// 
        /// </summary>
        int Top { get; set; }
        /// <summary>
        /// 
        /// </summary>
        string TableName { get; set; }
        /// <summary>
        /// 
        /// </summary>
        string Where { get; set; }
        /// <summary>
        /// 
        /// </summary>
        string OrderBy { get; set; }
        /// <summary>
        /// 
        /// </summary>
        string SelectColumns { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IOrderQuery<T> : IQuery<T>
    {

    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class Queryable<T> : IOrderQuery<T>
    {
        public SqlConnection Db { get; set; }
        public SqlTransaction Transaction { get; set; }
        public int? CommandTimeout { get; set; }
        public object WhereConditions { get; set; }
        public int Top { get; set; } = -1;
        public string TableName { get; set; }
        public string Where { get; set; }
        public string OrderBy { get; set; }
        public string SelectColumns { get; set; }
    }
}
