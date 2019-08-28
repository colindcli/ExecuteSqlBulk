# ExecuteSqlBulk

Bulk GetListByBulk/DeleteListByBulk for Sqlserver

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

using (var db = new SqlConnection("Data Source=.;Initial Catalog=SqlBulkTestDb;Integrated Security=True"))
{
    //GetList: SELECT TOP 2 * FROM Page ORDER BY PageLink ASC;
    var list = db.GetListByBulk<Page>(new
    {
        PageId = new List<int>()
        {
            1,
            2,
        }
    }).OrderBy(p=>p.PageLink).Take(2).ToList();

    //DeleteList: DELETE FROM Page WHERE PageName='PageName' AND (PageId=1 OR PageId=2)
    db.DeleteListByBulk<Page>(new
    {
        PageName = "PageName",
        PageId = new List<string>()
        {
            1,
            2
        }
    });
}
```
