# ExecuteSqlBulk - A .NET Bulk Insert/Update/Delete for Sqlserver

SqlServer 批量添加、批量修改、批量删除

----------

## NuGet Gallery

- [NuGet Gallery: ExecuteSqlBulk](https://www.nuget.org/packages/ExecuteSqlBulk/)


## How to use LiteDB

A quick example for storing and searching documents:

```C#
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
```

For more information, would you see [Example](https://github.com/colindcli/ExecuteSqlBulk/blob/master/ConsoleTest/Program.cs#L27)?

## License

ExecuteSqlBulk is provided under The [MIT](https://github.com/colindcli/ExecuteSqlBulk/blob/master/LICENSE) License.


Copyright (c) 2018 - Colindcli
