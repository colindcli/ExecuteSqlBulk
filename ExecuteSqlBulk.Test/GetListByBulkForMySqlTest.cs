
#if DEBUG
using Dapper;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using MySqlConnector;

namespace ExecuteSqlBulk.Test
{
    [TestClass]
    public class GetListByBulkForMySql
    {
        private static readonly string FilePath = Path.GetFullPath($"{AppDomain.CurrentDomain.BaseDirectory}/../../../App_Data/");
        private static string ConnStringMasterMysql => Config.ConnStringMasterMysql;
        private static readonly string ConnStringSqlBulkTestDbMysql = Config.ConnStringSqlBulkTestDbMysql;

        public GetListByBulkForMySql()
        {
            QueryConfig.SetDialect(Dialect.MySql);

            Setup();
            Excute();
        }

        [TestMethod]
        public void TestMethod1()
        {
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
                var num = db.Query<int>(@"SELECT COUNT(1) FROM Page p;").FirstOrDefault();
                Assert.IsTrue(num == Number);
            }
        }

        [TestMethod]
        public void TestMethod1A()
        {
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
                var li = new[] { 1, 2, 3, 4, 5 };
                var num = db.Query<int>(@"SELECT * FROM Page p where PageId IN @PageId;", new { PageId = li }).ToList().Count;
                Assert.IsTrue(num == 5, num.ToString());
            }
        }

        [TestMethod]
        public void TestMethod2()
        {
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
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
            using (var db = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                db.Open();
                var tran = db.BeginTransaction();
                foreach (var item in list)
                {
                    db.Execute("insert into Page(PageId, PageLink, PageName) value('" + item.PageId + "', '" + item.PageLink + "', '" + item.PageName + "');", null, tran);
                }
                tran.Commit();

                //row = db.BulkUpdate(list, p => new { p.PageName, p.PageLink }, p => new { p.PageId });
                //row = db.BulkDelete(list, p => new { p.PageId });
                //db.BulkDelete<Page>();
            }

            sw.Stop();
            //Console.WriteLine($"Execute time: {sw.ElapsedMilliseconds} ms, Row: {row}");
        }

        private static void Setup()
        {
            using (var db = new MySqlConnection(ConnStringMasterMysql))
            {
                db.Open();
                db.Execute(@"drop database if exists `SqlBulkTestDb`;create DATABASE SqlBulkTestDb;");
            }

            using (var connection = new MySqlConnection(ConnStringSqlBulkTestDbMysql))
            {
                connection.Open();
                connection.Execute(@"
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS `Page`;
CREATE TABLE `Page` (
  `PageId` int(11) NOT NULL,
  `PageName` varchar(50) DEFAULT NULL,
  `PageLink` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`PageId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                //connection.Execute(@"TRUNCATE TABLE Page;");
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
            if (QueryConfig.DialectServer == Dialect.SqlServer)
            {
                Assert.IsTrue(str == "[PageId] + ' ' + [PageName]");
            }
            else if (QueryConfig.DialectServer == Dialect.MySql)
            {
                Assert.IsTrue(str == "CONCAT(`PageId` , ' ' , `PageName`)");
            }
            else
            {
                Assert.IsTrue(false);
            }
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

            Assert.IsTrue(str == " WHERE CONCAT(`PageName` , ' ' , `PageLink`) LIKE CONCAT('%', @Keyword__0, '%') AND CONCAT(`PageName` , ' ' , `PageLink`) LIKE CONCAT('%', @Keyword__1, '%')");

            Assert.IsTrue(param.Count == 2);
        }
    }
}
#endif
