using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Data.Common;
using System.Data;

namespace Tests.Orion.Core.DB_DBConnectionTest
{
    [TestFixture]
    public class Open
    {
        [Test]
        public void 接続できるべき()
        {
            using (DBConnection connection = DBConnection.Open("System.Data.SqlClient", @"Data Source=.\SQLEXPRESS;AttachDbFilename=|DataDirectory|\DB_DBConnectionTest\Open.mdf;Integrated Security=True;User Instance=True;Min Pool Size=0"))
            {
                Assert.IsTrue(true);
            }
        }
    }

    public class DBConnection : IDisposable
    {
        private DbConnection Connection
        {
            get;
            set;
        }
        private DbProviderFactory ProviderFactory
        {
            get;
            set;
        }

        public static DBConnection Open(string provider, string connectionString)
        {
            DBConnection ret = new DBConnection();

            ret.ProviderFactory = DbProviderFactories.GetFactory(provider);

            ret.Connection = ret.ProviderFactory.CreateConnection();

            ret.Connection.ConnectionString = connectionString;

            ret.Connection.Open();

            return ret;
        }

        public DataSet FillDataSet(string sql, DBParameter param)
        {
            DataSet ret = new DataSet();
            using (DbDataAdapter adapter = this.ProviderFactory.CreateDataAdapter())
            using (DbCommand command = this.ProviderFactory.CreateCommand())
            {
                adapter.SelectCommand = command;
                command.Connection = this.Connection;
                command.CommandText = sql;

                
                foreach(KeyValuePair<string, object> p in param.ParameterList)
                {
                    DbParameter parameter = command.CreateParameter();
                    parameter.ParameterName = p.Key;
                    parameter.Value = p.Value;

                    command.Parameters.Add(parameter);
                }

                adapter.Fill(ret);
            }
            return ret;
        }
        public DataSet FillDataSet(string sql)
        {
            return this.FillDataSet(sql, new DBParameter());
        }


        public DataTable FillDataTable(string sql, DBParameter param)
        {
            DataTable ret = new DataTable();
            using (DbDataAdapter adapter = this.ProviderFactory.CreateDataAdapter())
            using (DbCommand command = this.ProviderFactory.CreateCommand())
            {
                adapter.SelectCommand = command;
                command.Connection = this.Connection;
                command.CommandText = sql;

                foreach (KeyValuePair<string, object> p in param.ParameterList)
                {
                    DbParameter parameter = command.CreateParameter();
                    parameter.ParameterName = p.Key;
                    parameter.Value = p.Value;

                    command.Parameters.Add(parameter);
                }

                adapter.Fill(ret);
            }

            return ret;
        }

        public DataTable FillDataTable(string sql)
        {
            return this.FillDataTable(sql, new DBParameter());
        }

        public void Dispose()
        {
            this.Connection.Dispose();
        }
    }
    public class DBParameter
    {
        public List<KeyValuePair<string, object>> ParameterList
        {
            get;
            set;
        }
        public DBParameter()
        {
            this.ParameterList = new List<KeyValuePair<string, object>>();
        }
        
        public void Add(string parameterName, object value)
        {
            this.ParameterList.Add(new KeyValuePair<string,object>(parameterName, value));
        }
    }
}
