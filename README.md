# ExecuteSqlBulk - Bulk Insert/Update/Delete for Sqlserver

SqlServer 批量添加、批量修改、批量删除

## NuGet Gallery

- [NuGet Gallery: ExecuteSqlBulk](https://www.nuget.org/packages/ExecuteSqlBulk/)


## Usage

A quick example:

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


## Performance

|   Bulk       |  20000 records |  200000 records |  1000000 records |
|------------- |--------:       |------------:    |--------:         |
|   Insert     |    217 ms      |  1176 ms        |   6529 ms        |
|   Update     |    251 ms      |  1586 ms        |   6595 ms        |
|   Delete     |    163 ms      |  1277 ms        |   6956 ms        |
|   Delete All |    9 ms        |  9 ms           |   13 ms          |

## License

ExecuteSqlBulk is provided under The [MIT](https://github.com/colindcli/ExecuteSqlBulk/blob/master/LICENSE) License.


Copyright (c) 2018 - Colindcli
