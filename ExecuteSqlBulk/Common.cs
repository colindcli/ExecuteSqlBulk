using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace ExecuteSqlBulk
{
    public class Common
    {
        public static string[] GetColumns(object obj)
        {
            return obj.GetType().GetProperties().Select(GetColumnName).ToArray();
        }

        public static DataTable GetDataTableFromFields<T>(IEnumerable<T> data, SqlBulkCopy sqlBulkCopy)
        {
            var dt = new DataTable();
            var listType = typeof(T).GetProperties();
            var list = new List<PropertiesModel>();
            foreach (var propertyInfo in listType)
            {
                var columnName = GetColumnName(propertyInfo);
                dt.Columns.Add(columnName, propertyInfo.PropertyType);
                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                list.Add(new PropertiesModel()
                {
                    PropertyInfo = propertyInfo,
                    ColumnName = columnName
                });
            }

            foreach (var value in data)
            {
                var dr = dt.NewRow();
                foreach (var item in list)
                {
                    dr[item.ColumnName] = item.PropertyInfo.GetValue(value, null);
                }
                dt.Rows.Add(dr);
            }

            return dt;
        }
        
        /// <summary>
        /// Gets the column name for the target database.  
        /// If the System.ComponentModel.DataAnnotations.ColumnAttribute is used
        /// it will attempt to use this value as the target column name.
        /// </summary>
        /// <param name="propertyInfo"></param>
        /// <returns></returns>
        public static string GetColumnName(PropertyInfo propertyInfo)
        {
            var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>(false);
            return columnAttribute != null ? columnAttribute.Name : propertyInfo.Name;
        }
    }
}