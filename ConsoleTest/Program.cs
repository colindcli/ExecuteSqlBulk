using ExecuteSqlBulk;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;

namespace ConsoleTest
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var list = new List<Page>();
            for (var i = 0; i < 200000; i++)
            {
                list.Add(new Page()
                {
                    PageId = i,
                    PageLink = (i + 5).ToString(),
                    PageName = (i + 4).ToString()
                });
            }

            var sw = new Stopwatch();
            sw.Start();
            var row = 0;
            using (var db = new SqlConnection("Data Source=.;uid=demo;pwd=123456;database=Demo;"))
            {
                db.BulkInsert(list);
                //row = db.BulkUpdate(list, p => new { p.PageName, p.PageLink }, p => new { p.PageId });
                //row = db.BulkDelete(list, p => new { p.PageId });
                //db.BulkDelete<Page>();
            }
            sw.Stop();
            Console.WriteLine($"Excute Time: {sw.ElapsedMilliseconds} ms, Row: {row}");
            Console.ReadKey();
        }
    }

    public class Page
    {
        public int PageId { get; set; }
        public string PageName { get; set; }
        public string PageLink { get; set; }
    }
}
