namespace ExecuteSqlBulk.Test
{
    public class Config
    {
        //sql server
        public static readonly string ConnStringMaster = $"Data Source=.,59157;Initial Catalog=Master;Integrated Security=True";
        public static readonly string ConnStringSqlBulkTestDb = $"Data Source=.,59157;Initial Catalog=SqlBulkTestDb;Integrated Security=True";

        //mysql
        public static readonly string ConnStringMasterMysql = $"Server=localhost;User ID=root;Password=123456;";
        public static readonly string ConnStringSqlBulkTestDbMysql = $"Server=localhost;User ID=root;Password=123456;Database=SqlBulkTestDb;";
    }
}
