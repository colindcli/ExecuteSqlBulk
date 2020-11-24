using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace ExecuteSqlBulk
{
    /// <summary>
    /// 
    /// </summary>
    internal class Common
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        internal static string[] GetColumns(object obj)
        {
            return obj.GetType().GetProperties().Select(GetColumnName).Where(p => p != null).ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="sqlBulkCopy"></param>
        /// <returns></returns>
        internal static DataTable GetDataTableFromFields<T>(IEnumerable<T> data, SqlBulkCopy sqlBulkCopy)
        {
            var dt = new DataTable();
            var listType = typeof(T).GetProperties();
            var list = new List<PropertiesModel>();
            foreach (var propertyInfo in listType)
            {
                var columnName = GetColumnName(propertyInfo);
                if (columnName == null)
                {
                    continue;
                }

                DataColumn column;
                if (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var typeArray = propertyInfo.PropertyType.GetGenericArguments();
                    var baseType = typeArray[0];
                    column = new DataColumn(columnName, baseType);
                }
                else
                {
                    column = new DataColumn(columnName, propertyInfo.PropertyType);
                }

                dt.Columns.Add(column);

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
                    dr[item.ColumnName] = item.PropertyInfo.GetValue(value, null) ?? DBNull.Value;
                }
                dt.Rows.Add(dr);
            }

            return dt;
        }

        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        /// <param name="sqlBulkCopy"></param>
        /// <param name="columnNames">列名</param>
        /// <returns></returns>
        internal static DataTable GetDataTableFromFields<T>(IEnumerable<T> data, SqlBulkCopy sqlBulkCopy, List<string> columnNames)
        {
            var listType = typeof(T).GetProperties();
            var list = new List<PropertiesModel>();
            foreach (var propertyInfo in listType)
            {
                var columnName = GetColumnName(propertyInfo);
                if (columnName == null)
                {
                    continue;
                }

                //仅写入指定列
                if (!columnNames.Exists(p => string.Equals(columnName, p, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                list.Add(new PropertiesModel()
                {
                    PropertyInfo = propertyInfo,
                    ColumnName = columnName
                });
            }

            var dt = new DataTable();
            //与指定列顺序一致
            var cols = new List<PropertiesModel>();
            foreach (var columnName in columnNames)
            {
                var obj = list.Find(p => string.Equals(p.ColumnName, columnName, StringComparison.OrdinalIgnoreCase));
                if (obj != null)
                {
                    var propertyInfo = obj.PropertyInfo;
                    DataColumn column;
                    if (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        var typeArray = propertyInfo.PropertyType.GetGenericArguments();
                        var baseType = typeArray[0];
                        column = new DataColumn(columnName, baseType);
                    }
                    else
                    {
                        column = new DataColumn(columnName, propertyInfo.PropertyType);
                    }
                    dt.Columns.Add(column);
                    //
                    sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                    //
                    cols.Add(obj);
                }
                else
                {
                    throw new Exception("缺少列");
                }
            }

            foreach (var value in data)
            {
                var dr = dt.NewRow();
                foreach (var item in cols)
                {
                    dr[item.ColumnName] = item.PropertyInfo.GetValue(value, null) ?? DBNull.Value;
                }
                dt.Rows.Add(dr);
            }

            return dt;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="propertyInfo"></param>
        /// <returns></returns>
        internal static string GetColumnName(PropertyInfo propertyInfo)
        {
            var columnAttributes = propertyInfo.GetCustomAttributes().ToList();
            if (columnAttributes.Exists(p => p is NotMappedAttribute))
            {
                return null;
            }

            var columnAttribute = columnAttributes.Find(p => p is ColumnAttribute);
            return columnAttribute != null ? ((ColumnAttribute)columnAttribute).Name : propertyInfo.Name;
        }
    }
}
