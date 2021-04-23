#if DEBUG
using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExecuteSqlBulk.Test
{
    [TestClass]
    public class SqlBulkExtTest
    {
        [TestMethod]
        public void TestMethod1()
        {
            Expression<Func<Aa, object>> fun = p => p.Id;
            var cols = fun.GetColumns();

            var res = string.Join("|", cols);

            Assert.IsTrue(res == "Id", res);
        }

        [TestMethod]
        public void TestMethod2()
        {
            Expression<Func<Aa, object>> fun = p => new { p.Id, p.Ticks };
            var cols = fun.GetColumns();

            var res = string.Join("|", cols);

            Assert.IsTrue(res == "Id|Ticks", res);
        }

        public class Aa
        {
            public Guid Id { get; set; }
            public long Ticks { get; set; }
        }
    }
}
#endif
