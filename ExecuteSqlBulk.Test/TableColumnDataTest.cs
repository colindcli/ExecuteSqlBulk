//#if DEBUG
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Linq;
//using ExecuteSqlBulk.Data;

//namespace ExecuteSqlBulk.Test
//{
//    [TestClass]
//    public class TableColumnDataTest
//    {
//        [TestMethod]
//        public void TestMethod1()
//        {
//            var biz = new TableColumnData(Config.ConnStringSqlBulkTestDb);
//            var list = biz.GetDataList();
//            var table = TableColumnData.GetTables(list);
//            var col = TableColumnData.GetColumns(list, table.OrderBy(p => p.TableName).FirstOrDefault());

//            Assert.IsTrue(list.Count > 0 && table.Count > 0 && col.Count > 0);
//        }
//    }
//}
//#endif
