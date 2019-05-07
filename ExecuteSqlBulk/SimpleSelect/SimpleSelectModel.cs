using System.Data.SqlClient;

namespace ExecuteSqlBulk.SimpleSelect
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SimpleSelectModel<T>
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
