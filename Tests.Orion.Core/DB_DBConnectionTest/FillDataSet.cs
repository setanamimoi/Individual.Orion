using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Data;

namespace Tests.Orion.Core.DB_DBConnectionTest
{
    [TestFixture]
    public class FillDataSet
    {
        [Test]
        public void SQLが実行されてDataSetに照会内容がセットされるべき()
        {
            DataSet dataSet = null;
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
            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\FillDataSet.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                dataSet = connection.FillDataSet(sql);
            }

            Assert.AreEqual(1, dataSet.Tables.Count);

            DataTable dataTable = dataSet.Tables.Cast<DataTable>().First();
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
        public void 複数のSQLが実行されてDataSetに照会内容がセットされるべき()
        {
            DataSet dataSet = null;
            string sql = @"
                SELECT
                    ColumnA
                    ,ColumnB
                    ,ColumnC
                FROM
                    TestTable1
                ORDER BY
                    ColumnA
                ;
                SELECT
                    ColumnA
                    ,ColumnB
                    ,ColumnC
                FROM
                    TestTable1
                ORDER BY
                    ColumnA DESC
";
            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\FillDataSet.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                dataSet = connection.FillDataSet(sql);
            }

            Assert.AreEqual(2, dataSet.Tables.Count);

            {
                DataTable dataTable = dataSet.Tables.Cast<DataTable>().First();
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

            {
                DataTable dataTable = dataSet.Tables[1];
                Assert.AreEqual(4, dataTable.Rows.Count);
                {
                    DataRow row = dataTable.Rows[0];
                    Assert.AreEqual(4, row["ColumnA"]);
                    Assert.AreEqual(5, row["ColumnB"]);
                    Assert.AreEqual("まみむめもやゆよわを", row["ColumnC"]);
                }

                {
                    DataRow row = dataTable.Rows[1];
                    Assert.AreEqual(3, row["ColumnA"]);
                    Assert.AreEqual(4, row["ColumnB"]);
                    Assert.AreEqual("なにぬねのはひふへほ", row["ColumnC"]);
                }

                {
                    DataRow row = dataTable.Rows[2];
                    Assert.AreEqual(2, row["ColumnA"]);
                    Assert.AreEqual(3, row["ColumnB"]);
                    Assert.AreEqual("さしすせそたちつてと", row["ColumnC"]);
                }

                Assert.AreEqual(4, dataTable.Rows.Count);
                {
                    DataRow row = dataTable.Rows[3];
                    Assert.AreEqual(1, row["ColumnA"]);
                    Assert.AreEqual(2, row["ColumnB"]);
                    Assert.AreEqual("あいうえおかきくけこ", row["ColumnC"]);
                }
            }
        }

        [Test]
        public void パラメータを指定して複数のSQLが実行されてDataSetに照会内容がセットされるべき()
        {
            DataSet dataSet = null;
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
                ;
                SELECT
                    ColumnA
                    ,ColumnB
                    ,ColumnC
                FROM
                    TestTable1
                WHERE
                    ColumnA = @Two
                ORDER BY
                    ColumnA DESC
";
            DBParameter param = new DBParameter();
            param.Add("@One", 1);
            param.Add("@Two", 2);

            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\FillDataSet.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                dataSet = connection.FillDataSet(sql, param);
            }

            Assert.AreEqual(2, dataSet.Tables.Count);

            {
                DataTable dataTable = dataSet.Tables.Cast<DataTable>().First();
                Assert.AreEqual(1, dataTable.Rows.Count);
                {
                    DataRow row = dataTable.Rows[0];
                    Assert.AreEqual(1, row["ColumnA"]);
                    Assert.AreEqual(2, row["ColumnB"]);
                    Assert.AreEqual("あいうえおかきくけこ", row["ColumnC"]);
                }
            }

            {
                DataTable dataTable = dataSet.Tables[1];
                {
                    DataRow row = dataTable.Rows[0];
                    Assert.AreEqual(2, row["ColumnA"]);
                    Assert.AreEqual(3, row["ColumnB"]);
                    Assert.AreEqual("さしすせそたちつてと", row["ColumnC"]);
                }
            }
        }
    }
}
