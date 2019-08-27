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
    public class DeleteListByBulkTest
    {
        private static readonly string FilePath = Path.GetFullPath($"{AppDomain.CurrentDomain.BaseDirectory}/../../App_Data/");

        public DeleteListByBulkTest()
        {
            Setup();
            Excute();
        }

        [TestMethod]
        public void TestMethod1()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                //
                Assert.IsTrue(db.Query<int>(@"SELECT COUNT(1) FROM dbo.Category p;").FirstOrDefault() == Number);

                //
                Test1(db);
                Test2(db);
                Test3(db);
                Test4(db);
                Test5(db);
            }
        }

        private void Test1(SqlConnection db)
        {
            var where = new
            {
                CategoryName = new List<string>()
                {
                    "Name_0",
                    "Name_1"
                }
            };
            db.DeleteListByBulk<Category>(where);

            var list = db.GetListByBulk<Category>(null).OrderBy(p => p.CategoryId).ToList();
            var json = JsonConvert.SerializeObject(list);

            //
            var txt = File.ReadAllText($"{FilePath}delete_1_result.json");
            var rows = JsonConvert.DeserializeObject<List<Category>>(txt);

            var b = new CompareLogic().Compare(list, rows);
            Assert.IsTrue(b.AreEqual);
        }

        private void Test2(SqlConnection db)
        {
            var where = new
            {
                CategoryName = new List<string>()
                {
                    "Name_2",
                    "Name_3"
                },
                CategoryLink = new List<string>()
                {
                    "Link_2",
                    "Link_4"
                }
            };
            db.DeleteListByBulk<Category>(where);

            var list = db.GetListByBulk<Category>(null).OrderBy(p => p.CategoryId).ToList();
            var json = JsonConvert.SerializeObject(list);

            //
            var txt = File.ReadAllText($"{FilePath}delete_2_result.json");
            var rows = JsonConvert.DeserializeObject<List<Category>>(txt);

            var b = new CompareLogic().Compare(list, rows);
            Assert.IsTrue(b.AreEqual);
        }

        private void Test3(SqlConnection db)
        {
            var where = new
            {
                CategoryName = "Name_3",
                CategoryLink = new List<string>()
                {
                    "Link_3",
                    "Link_4"
                }
            };
            db.DeleteListByBulk<Category>(where);

            var list = db.GetListByBulk<Category>(null).OrderBy(p => p.CategoryId).ToList();
            var json = JsonConvert.SerializeObject(list);

            //
            var txt = File.ReadAllText($"{FilePath}delete_3_result.json");
            var rows = JsonConvert.DeserializeObject<List<Category>>(txt);

            var b = new CompareLogic().Compare(list, rows);
            Assert.IsTrue(b.AreEqual);
        }

        private void Test4(SqlConnection db)
        {
            db.DeleteListByBulk<Category>(new {});

            var list = db.GetListByBulk<Category>(null).OrderBy(p => p.CategoryId).ToList();

            Assert.IsTrue(list.Count == 0);
        }

        private void Test5(SqlConnection db)
        {
            Excute();

            db.DeleteListByBulk<Category>(null);

            var list = db.GetListByBulk<Category>(null).OrderBy(p => p.CategoryId).ToList();

            Assert.IsTrue(list.Count == 0);
        }

        private static readonly int Number = 20;

        private static void Excute()
        {
            var list = new List<Category>();
            for (var i = 0; i < Number; i++)
            {
                list.Add(new Category()
                {
                    CategoryId = i,
                    CategoryLink = $"Link_{i % 10}",
                    CategoryName = $"Name_{i}"
                });
            }

            var sw = new Stopwatch();
            sw.Start();
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                db.BulkInsert(list);
            }

            sw.Stop();
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
IF(NOT EXISTS(SELECT * FROM sys.objects o WHERE o.name='Category'))
CREATE TABLE [dbo].[Category](
	[CategoryId] [INT] NOT NULL,
	[CategoryName] [VARCHAR](50) NULL,
	[CategoryLink] [VARCHAR](50) NULL,
 CONSTRAINT [PK_Category_1] PRIMARY KEY CLUSTERED 
(
	[CategoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY];");
                connection.Execute(@"TRUNCATE TABLE dbo.Category;");
            }
            //Console.WriteLine("Created database");
        }

        public class Category
        {
            public int CategoryId { get; set; }
            public string CategoryName { get; set; }
            public string CategoryLink { get; set; }
        }
    }
}
