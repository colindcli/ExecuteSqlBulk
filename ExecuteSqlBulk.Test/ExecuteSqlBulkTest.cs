using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ExecuteSqlBulk.Test
{
    [TestClass]
    public class ExecuteSqlBulkTest
    {
        private static readonly string FilePath = Path.GetFullPath($"{AppDomain.CurrentDomain.BaseDirectory}/../../App_Data/");

        public ExecuteSqlBulkTest()
        {
            Setup();
            Excute();
        }

        /// <summary>
        /// 测试批量添加
        /// </summary>
        [TestMethod]
        public void TestMethod1()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                Assert.IsTrue(db.Query<int>(@"SELECT COUNT(1) FROM dbo.[User] p;").FirstOrDefault() == Number);
            }
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        [TestMethod]
        public void TestMethod2()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").ToList();
                list.ForEach(p => p.UserName = "UserName");
                db.BulkUpdate(list, p => new { p.UserName, p.Content }, p => new { p.UserId });

                Assert.IsTrue(db.Query<User>(@"SELECT * FROM dbo.[User] p;").Count(p => p.UserName == "UserName") == Number);
            }
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        [TestMethod]
        public void TestMethod3()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").Take(10).ToList();
                db.BulkDelete(list, p => new { p.UserId });

                Assert.IsTrue(db.Query<User>(@"SELECT * FROM dbo.[User] p;").Count() == Number - 10);
            }
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        [TestMethod]
        public void TestMethod4()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkDelete<User>();

                Assert.IsTrue(!db.Query<User>(@"SELECT * FROM dbo.[User] p;").Any());
            }
        }

        private static readonly int Number = 20;

        private static void Excute()
        {
            var list = new List<User>();
            for (var i = 0; i < Number; i++)
            {
                list.Add(new User()
                {
                    UserId = i,
                    Content = $"Link_{i % 10}",
                    UserName = $"Name_{i}"
                });
            }

            var sw = new Stopwatch();
            sw.Start();
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkInsert(list);
                //row = db.BulkUpdate(list, p => new { p.UserName, p.Content }, p => new { p.UserId });
                //row = db.BulkDelete(list, p => new { p.UserId });
                //db.BulkDelete<User>();
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
IF(NOT EXISTS(SELECT * FROM sys.objects o WHERE o.name='User'))
CREATE TABLE [dbo].[User](
	[UserId] [INT] NOT NULL,
	[UserName] [VARCHAR](50) NULL,
	[Content] [VARCHAR](50) NULL,
 CONSTRAINT [PK_User_1] PRIMARY KEY CLUSTERED 
(
	[UserId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];");
                connection.Execute(@"TRUNCATE TABLE dbo.[User];");
            }
            //Console.WriteLine("Created database");
        }

        public partial class User
        {
            /// <summary>
            /// 设置列名
            /// </summary>
            //[Column("UserId")]
            public int UserId { get; set; }
            public string UserName { get; set; }
            public string Content { get; set; }
        }

        public partial class User
        {
            /// <summary>
            /// 排除列
            /// </summary>
            [NotMapped]
            public string UserDepth { get; set; }
        }
    }
}
