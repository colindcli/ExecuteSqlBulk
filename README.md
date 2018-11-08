# ExecuteSqlBulk
BulkInsert, BulkDelete, BulkUpdate for sqlserver

> Nuget Install

- [https://www.nuget.org/packages/ExecuteSqlBulk/](https://www.nuget.org/packages/ExecuteSqlBulk/)

> Examples

    public class Page
    {
        [key]
        public int PageId { get; set; }
        public string PageName { get; set; }
        public string PageLink { get; set; }
    }
    
    using (var db = new SqlConnection("Data Source=.;uid=;pwd=;database=;"))
    {
        //insert
        db.BulkInsert(list);
        
        //update
        row = db.BulkUpdate(list, p => new { p.PageName, p.PageLink }, p => new { p.PageId });
        
        //delete
        row = db.BulkDelete(list, p => new { p.PageId });
        
        //delete all
        db.BulkDelete<Page>();
    }
