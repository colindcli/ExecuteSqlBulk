using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace ExecuteSqlBulk.Test
{
    [TestClass]
    public class ExecuteSqlBulkTest
    {
        public ExecuteSqlBulkTest()
        {
            Setup();
            Excute();
        }

        [TestMethod]
        public void TestMethod1()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                Assert.IsTrue(db.Query<int>(@"SELECT COUNT(1) FROM dbo.Page p;").FirstOrDefault() == Number);
            }
        }

        private static readonly int Number = 20;

        private static void Excute()
        {
            var list = new List<Page>();
            for (var i = 0; i < Number; i++)
            {
                list.Add(new Page()
                {
                    PageId = i,
                    PageLink = $"Link_{i % 10}",
                    PageName = $"Name_{i}"
                });
            }

            var sw = new Stopwatch();
            sw.Start();
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkInsert(list);
                //row = db.BulkUpdate(list, p => new { p.PageName, p.PageLink }, p => new { p.PageId });
                //row = db.BulkDelete(list, p => new { p.PageId });
                //db.BulkDelete<Page>();
            }

            sw.Stop();
            //Console.WriteLine($"Execute time: {sw.ElapsedMilliseconds} ms, Row: {row}");
        }

        private static readonly string ConnStringMaster = $"Data Source=.;Initial Catalog=Master;Integrated Security=True";
        private static readonly string ConnStringSqlBulkTestDb = $"Data Source=.;Initial Catalog=SqlBulkTestDb;Integrated Security=True";

        private static void Setup()
        {
            using (var db = new SqlConnection(ConnStringMaster))
            {
                db.Open();
                db.Execute(@"IF(NOT EXISTS(SELECT * FROM sys.databases d WHERE d.name='SqlBulkTestDb')) CREATE DATABASE SqlBulkTestDb;");
            }

            using (var connection = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                connection.Open();
                connection.Execute(@"
IF(NOT EXISTS(SELECT * FROM sys.objects o WHERE o.name='Page'))
CREATE TABLE [dbo].[Page](
	[PageId] [INT] NOT NULL,
	[PageName] [VARCHAR](50) NULL,
	[PageLink] [VARCHAR](50) NULL,
 CONSTRAINT [PK_Page_1] PRIMARY KEY CLUSTERED 
(
	[PageId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];");
                connection.Execute(@"TRUNCATE TABLE dbo.Page;");
            }
            //Console.WriteLine("Created database");
        }

        public class Page
        {
            public int PageId { get; set; }
            public string PageName { get; set; }
            public string PageLink { get; set; }
        }
    }
}
