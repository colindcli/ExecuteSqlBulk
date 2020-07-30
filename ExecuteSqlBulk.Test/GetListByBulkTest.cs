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
    public class GetListByBulkTest
    {
        private static readonly string FilePath = Path.GetFullPath($"{AppDomain.CurrentDomain.BaseDirectory}/../../App_Data/");
        private static string ConnStringMaster => Config.ConnStringMaster;
        private static readonly string ConnStringSqlBulkTestDb = Config.ConnStringSqlBulkTestDb;

        public GetListByBulkTest()
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

        [TestMethod]
        public void TestMethod2()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(new
                {
                    PageId = new List<int>()
                    {
                        1,
                        2,
                        3,
                        4,
                        5
                    }
                }).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_1_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod3()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(new
                {
                    PageId = new List<int>()
                    {
                        1,
                        2,
                        3,
                        4,
                        5
                    }
                }).Take(2).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_2_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod3A()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(new
                {
                    PageId = new List<int>()
                    {
                        1,
                        2,
                        3,
                        4,
                        5
                    }
                }).OrderBy(p => p.PageLink).Take(2).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_2_result1.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod3B()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(new
                {
                    PageId = new List<int>()
                    {
                        1,
                        2,
                        3,
                        4,
                        5
                    }
                }).Take(2).OrderBy(p => p.PageLink).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_2_result1.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod4()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(new
                {

                }).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_3_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod5()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_3_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod5A()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null, p => new { p.PageName, p.PageLink }).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_3A_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod6()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null).OrderBy(p => p.PageLink).ThenBy(p => p.PageName).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_4_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod7()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null).OrderBy(p => p.PageLink).ThenByDescending(p => p.PageName).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_5_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod8()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null).OrderByDescending(p => p.PageLink).ThenBy(p => p.PageName).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_6_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod9()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = db.GetListByBulk<Page>(null).OrderByDescending(p => p.PageLink).ThenByDescending(p => p.PageName).ToList();

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_7_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod10()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = new List<Page>();

                var item1 = db.GetListByBulk<Page>(null).OrderByDescending(p => p.PageLink).ThenBy(p => p.PageName).FirstOrDefault();
                var item2 = db.GetListByBulk<Page>(null).OrderByDescending(p => p.PageLink).ThenByDescending(p => p.PageName).FirstOrDefault();
                var item3 = db.GetListByBulk<Page>(null).OrderBy(p => p.PageLink).OrderBy(p => p.PageName).FirstOrDefault();
                var item4 = db.GetListByBulk<Page>(null).OrderBy(p => p.PageLink).ThenByDescending(p => p.PageName).FirstOrDefault();

                list.Add(item1);
                list.Add(item2);
                list.Add(item3);
                list.Add(item4);

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_8_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
            }
        }

        [TestMethod]
        public void TestMethod14()
        {
            using (var db = new SqlConnection(ConnStringSqlBulkTestDb))
            {
                var list = new List<Page>();

                var item1 = db.GetListByBulkLike<Page>(p => new { p.PageName, p.PageLink }, new List<string>() { "name_0", "link_0" }).OrderByDescending(p => p.PageLink).ThenByDescending(p => p.PageId).ToList();
                var item2 = db.GetListByBulkLike<Page>(p => new { p.PageName, p.PageLink }, new List<string>() { "link_8" }).FirstOrDefault();

                list.AddRange(item1);
                list.Add(item2);

                var json = JsonConvert.SerializeObject(list);

                //
                var txt = File.ReadAllText($"{FilePath}file_9_result.json");
                var rows = JsonConvert.DeserializeObject<List<Page>>(txt);

                var b = new CompareLogic().Compare(list, rows);
                Assert.IsTrue(b.AreEqual);
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

        [TestMethod]
        public void TestMethod11()
        {
            var str = QueryableBuilder.GetQueryColumn<Page>(p => new { p.PageId, p.PageName });
            Assert.IsTrue(str == "[PageId] + ' ' + [PageName]");
        }

        [TestMethod]
        public void TestMethod12()
        {
            var str = QueryableBuilder.GetQueryColumn<Page>(p => new { });
            Assert.IsTrue(str == "");
        }

        [TestMethod]
        public void TestMethod13()
        {
            var str = QueryableBuilder.GetWhere<Page>(p => new { p.PageName, p.PageLink }, new List<string>()
            {
                "test",
                "test2"
            }, out var param);

            Assert.IsTrue(str == " WHERE [PageName] + ' ' + [PageLink] LIKE '%' + @Keyword__0 + '%' AND [PageName] + ' ' + [PageLink] LIKE '%' + @Keyword__1 + '%'");

            Assert.IsTrue(param.Count == 2);
        }
    }
}
#endif