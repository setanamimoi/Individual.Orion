using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Data;

namespace Tests.Orion.Core.DB_DBConnectionTest
{
    [TestFixture]
    public class FillDataTable
    {
        [Test]
        public void SQLが実行されてDataTableに照会内容がセットされるべき()
        {
            DataTable dataTable = null;
            string sql = @"
                SELECT
                    ColumnA
                    ,ColumnB
                    ,ColumnC
                FROM
                    TestTable1
                ORDER BY
                    ColumnA
";
            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\FillDataTable.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                dataTable = connection.FillDataTable(sql);
            }

            Assert.AreEqual(4, dataTable.Rows.Count);
            {
                DataRow row = dataTable.Rows[0];
                Assert.AreEqual(1, row["ColumnA"]);
                Assert.AreEqual(2, row["ColumnB"]);
                Assert.AreEqual("あいうえおかきくけこ", row["ColumnC"]);
            }
            {
                DataRow row = dataTable.Rows[1];
                Assert.AreEqual(2, row["ColumnA"]);
                Assert.AreEqual(3, row["ColumnB"]);
                Assert.AreEqual("さしすせそたちつてと", row["ColumnC"]);
            }

            {
                DataRow row = dataTable.Rows[2];
                Assert.AreEqual(3, row["ColumnA"]);
                Assert.AreEqual(4, row["ColumnB"]);
                Assert.AreEqual("なにぬねのはひふへほ", row["ColumnC"]);
            }
            {
                DataRow row = dataTable.Rows[3];
                Assert.AreEqual(4, row["ColumnA"]);
                Assert.AreEqual(5, row["ColumnB"]);
                Assert.AreEqual("まみむめもやゆよわを", row["ColumnC"]);
            }
        }

        [Test]
        public void パラメータを指定してSQLが実行されてDataTableに照会内容がセットされるべき()
        {
            DataTable dataTable = null;
            string sql = @"
                SELECT
                    ColumnA
                    ,ColumnB
                    ,ColumnC
                FROM
                    TestTable1
                WHERE
                    ColumnA = @One
                ORDER BY
                    ColumnA
";
            DBParameter param = new DBParameter();
            param.Add("@One", 1);

            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\FillDataSet.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                dataTable = connection.FillDataTable(sql, param);
            }

            {
                Assert.AreEqual(1, dataTable.Rows.Count);
                {
                    DataRow row = dataTable.Rows[0];
                    Assert.AreEqual(1, row["ColumnA"]);
                    Assert.AreEqual(2, row["ColumnB"]);
                    Assert.AreEqual("あいうえおかきくけこ", row["ColumnC"]);
                }
            }
        }
    }
}
