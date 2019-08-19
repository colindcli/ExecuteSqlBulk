using Dapper;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
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
        /// 测试
        /// </summary>
        [TestMethod]
        public void TestMethod1()
        {
            TestBulkInsert();

            TestBulkUpdate();
            TestBulkUpdate2();
            TestBulkUpdate3();

            TestBlukDelete();
            TestBlukDelete2();

            TestDeleteAll();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        private void Test(string fileName)
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                // 验证
                var user = db.Query<User>(@"SELECT * FROM dbo.[User] p;").ToList();
                var json = JsonConvert.SerializeObject(user);
                var txt = File.ReadAllText($"{FilePath}{fileName}");
                var rows = JsonConvert.DeserializeObject<List<User>>(txt);

                var b = new CompareLogic().Compare(user, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        /// <summary>
        /// 测试批量添加
        /// </summary>
        private void TestBulkInsert()
        {
            Test("bulk_1_result.json");
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        private void TestBulkUpdate()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").ToList();
                list.ForEach(p =>
                {
                    p.UserName = $"{p.UserName}_Test";
                    p.Content = $"{p.Content}_Test";
                });
                db.BulkUpdate(list, p => new { p.UserName, p.Content }, p => new { p.UserId });
            }

            Test("bulk_2_result.json");
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        private void TestBulkUpdate2()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").ToList();
                list.ForEach(p =>
                {
                    p.Content = $"{p.Content}_View";
                });
                db.BulkUpdate(list, p => new { p.Content }, p => new { p.UserId, p.UserName });
            }

            Test("bulk_3_result.json");
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        private void TestBulkUpdate3()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").ToList();
                list.ForEach(p =>
                {
                    p.Content = $"{p.Content}_3";
                });
                db.BulkUpdate(list, p => p.Content, p => p.UserId);
            }

            Test("bulk_4_result.json");
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        public void TestBlukDelete()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").Where(p => p.UserId >= 0 && p.UserId < 5).ToList();
                db.BulkDelete(list, p => new { p.UserId, p.UserName });
            }

            Test("bulk_5_result.json");
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        public void TestBlukDelete2()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<User>(@"SELECT * FROM dbo.[User] p;").Where(p => p.UserId >= 5 && p.UserId < 10).ToList();
                db.BulkDelete(list, p => p.UserId);
            }

            Test("bulk_6_result.json");
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        public void TestDeleteAll()
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
