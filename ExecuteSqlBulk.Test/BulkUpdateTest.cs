#if DEBUG
using Dapper;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ExecuteSqlBulk.Test
{
    [TestClass]
    public class BulkUpdateTest
    {
        private static readonly string FilePath = Path.GetFullPath($"{AppDomain.CurrentDomain.BaseDirectory}/../../../App_Data/");
        private static string ConnStringMaster => Config.ConnStringMaster;
        private static readonly string ConnStringSqlBulkTestDb = Config.ConnStringSqlBulkTestDb;

        public BulkUpdateTest()
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
                var product = db.Query<Product>(@"SELECT * FROM dbo.[Product] p;").ToList();
                var json = JsonConvert.SerializeObject(product);
                var txt = File.ReadAllText($"{FilePath}{fileName}");
                var rows = JsonConvert.DeserializeObject<List<Product>>(txt);

                var b = new CompareLogic().Compare(product, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        /// <summary>
        /// 测试批量添加
        /// </summary>
        private void TestBulkInsert()
        {
            Test("update_1_result.json");
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        private void TestBulkUpdate()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<Product>(@"SELECT * FROM dbo.[Product] p;").ToList();
                list.ForEach(p =>
                {
                    p.Price = null;
                });
                db.BulkUpdate(list, p => new { p.Price }, p => new { p.ProductId });
            }

            Test("update_2_result.json");
        }

        /// <summary>
        /// 测试批量更新
        /// </summary>
        private void TestBulkUpdate2()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.Query<Product>(@"SELECT * FROM dbo.[Product] p;").ToList();
                list.ForEach(p =>
                {
                    p.Price = 2;
                });
                db.BulkUpdate(list, p => new { p.Price }, p => p.ProductId);
            }

            Test("update_3_result.json");
        }

        /// <summary>
        /// 测试批量删除
        /// </summary>
        public void TestDeleteAll()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkDelete<Product>();

                Assert.IsTrue(!db.Query<Product>(@"SELECT * FROM dbo.[Product] p;").Any());
            }
        }

        private static readonly int Number = 20;

        private static void Excute()
        {
            var list = new List<Product>();
            for (var i = 0; i < Number; i++)
            {
                list.Add(new Product()
                {
                    ProductId = i,
                    Price = i % 2 == 0 ? (decimal?)null : i,
                    CreateDate = Convert.ToDateTime("2019-09-20")
                });
            }

            var sw = new Stopwatch();
            sw.Start();
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkInsert(list);
            }

            sw.Stop();
            //Console.WriteLine($"Execute time: {sw.ElapsedMilliseconds} ms, Row: {row}");
        }

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
IF(NOT EXISTS(SELECT * FROM sys.objects o WHERE o.name='Product'))
CREATE TABLE [dbo].[Product](
    [ProductId] [int] IDENTITY(1,1) NOT NULL,
    [Price] [decimal](18, 2) NULL,
    [CreateDate] [datetime] NOT NULL,
 CONSTRAINT [PK_Product] PRIMARY KEY CLUSTERED 
(
    [ProductId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];");
                connection.Execute(@"TRUNCATE TABLE dbo.[Product];");
            }
            //Console.WriteLine("Created database");
        }

        public class Product
        {
            /// <summary>
            /// 设置列名
            /// </summary>
            public int ProductId { get; set; }
            public decimal? Price { get; set; }

            public DateTime CreateDate { get; set; }
        }
    }
}
#endif
