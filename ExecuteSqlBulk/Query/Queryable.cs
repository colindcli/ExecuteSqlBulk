using System.Data;

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
        IDbConnection Db { get; set; }
        /// <summary>
        /// 
        /// </summary>
        IDbTransaction Transaction { get; set; }
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
        public IDbConnection Db { get; set; }
        public IDbTransaction Transaction { get; set; }
        public int? CommandTimeout { get; set; }
        public object WhereConditions { get; set; }
        public int Top { get; set; } = -1;
        public string TableName { get; set; }
        public string Where { get; set; }
        public string OrderBy { get; set; }
        public string SelectColumns { get; set; }
    }
}
