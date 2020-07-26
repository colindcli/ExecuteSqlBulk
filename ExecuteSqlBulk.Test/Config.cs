using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecuteSqlBulk.Test
{
    public class Config
    {
        public static readonly string ConnStringMaster = $"Data Source=.,59157;Initial Catalog=Master;Integrated Security=True";
        public static readonly string ConnStringSqlBulkTestDb = $"Data Source=.,59157;Initial Catalog=SqlBulkTestDb;Integrated Security=True";
    }
}
