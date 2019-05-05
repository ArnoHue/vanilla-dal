using System;
using System.Collections;
using System.Data;
using System.Text;
using VanillaDAL.Config;

namespace VanillaDAL {

	internal abstract class DBAccessorBase : IDBAccessor {

		protected abstract IDbDataAdapter CreateDataAdapterInternal();

		protected abstract IDbConnection CreateConnectionInternal();

		protected abstract IDbCommand CreateCommandInternal();

		protected abstract IDbDataParameter CreateDbParameter(Parameter param,
		                                                      ConfigParameterType configParamType);

		protected IDbDataParameter CreateDbParameter(DataColumn col) {
			return CreateDbParameter(col, null, col.ColumnName);
		}

		protected IDbDataParameter CreateDbParameter(DataColumn col, object val) {
			return CreateDbParameter(col, val, col.ColumnName);
		}

		protected abstract IDbDataParameter CreateDbParameter(DataColumn col, object val, string paramName);

		protected abstract string GetDbParameterName(string name);

		protected virtual string GetDbColumnName(string name) {
			return name;
		}

		protected virtual string GetIdentityParameterName() {
			return null;
		}

		protected virtual bool SupportsRefreshAfterUpdate() {
			return false;
		}

		protected virtual bool RequiresDuplicateParameters() 
		{
			return false;
		}

		protected virtual string GetDbTableName(string name) 
		{
			return name;
		}

		protected abstract int UpdateInternal(IDbDataAdapter adpt, DataRow[] rows);

		protected abstract void FillInternal(IDbDataAdapter adpt, DataTable table);

		protected abstract void FillSchemaInternal(IDbDataAdapter adpt, DataTable table, SchemaType type);

		protected VanillaConfig config;

		private static DictionaryEntry[] TYPE_TO_XML_LIST = new DictionaryEntry[] {
			new DictionaryEntry(typeof (bool),
			                    ConfigParameterType.
			                    	Boolean),
			new DictionaryEntry(typeof (byte),
			                    ConfigParameterType.Byte),
			new DictionaryEntry(typeof (byte[]),
			                    ConfigParameterType.
			                    	ByteArray),
			new DictionaryEntry(typeof (DateTime),
			                    ConfigParameterType.
			                    	DateTime),
			new DictionaryEntry(typeof (decimal),
			                    ConfigParameterType.
			                    	Decimal),
			new DictionaryEntry(typeof (double),
			                    ConfigParameterType.Double)
			,
			new DictionaryEntry(typeof (Guid),
			                    ConfigParameterType.Guid),
			new DictionaryEntry(typeof (short),
			                    ConfigParameterType.Int16)
			,
			new DictionaryEntry(typeof (int),
			                    ConfigParameterType.Int32)
			,
			new DictionaryEntry(typeof (long),
			                    ConfigParameterType.Int64)
			,
			new DictionaryEntry(typeof (string),
			                    ConfigParameterType.String)
            ,
            new DictionaryEntry(typeof (DBNull),
                                ConfigParameterType.Variant)
        };

		private static Hashtable TYPE_TO_XML_MAP = new Hashtable();

		static DBAccessorBase() {
			foreach (DictionaryEntry entry in TYPE_TO_XML_LIST) {
				TYPE_TO_XML_MAP[entry.Key] = entry.Value;
			}
		}

		protected DBAccessorBase(VanillaConfig _config) {
			config = _config;
		}

		internal static ConfigParameterType GetConfigParameterType(Type type) {
			if (TYPE_TO_XML_MAP.ContainsKey(type)) {
				return (ConfigParameterType) TYPE_TO_XML_MAP[type];
			}
			return ConfigParameterType.Undefined;
		}

		public IDbCommand CreateCommand(CommandParameter param) {
			if (param.Statement != null && param.Parameters != null) {
				return CreateCommandInternal(param.Statement, param.Parameters, param.Transaction);
			}
			else if (param.Transaction != null) {
				return CreateCommandInternal(param.Transaction);
			}
			else {
				return CreateCommandInternal();
			}
		}

		public IDbConnection CreateConnection() {
			return CreateConnectionInternal();
		}

		public IDbDataAdapter CreateDataAdapter() {
			return CreateDataAdapterInternal();
		}

		private IDbCommand CreateCommandInternal(IDbTransaction transaction) {
			IDbCommand cmd = CreateCommandInternal();
			if (transaction != null) {
				cmd.Connection = transaction.Connection;
				cmd.Transaction = transaction;
			}
			else {
				IDbTransaction scopeTrans = TransactionScope.GetCurrentTransaction();
				if (scopeTrans != null) {
					cmd.Connection = scopeTrans.Connection;
					cmd.Transaction = scopeTrans;
				}
				else {				
					cmd.Connection = CreateConnectionInternal();
				}
			}
			return cmd;
		}

		private void AddParameters(ConfigStatement stmt, ParameterList paramValues, IDbCommand cmd) {
			foreach (ConfigParameter xmlParameter in stmt.Parameters) {
				if (!paramValues.ContainsParameter(xmlParameter.Name)) {
					throw new VanillaExecutionException(
						string.Format("Parameter [{0}] does not exist in parameter list", xmlParameter.Name));
				}
				cmd.Parameters.Add(
					CreateDbParameter(paramValues.GetParameter(xmlParameter.Name), xmlParameter.Type));
			}
		}

		private IDbCommand CreateCommandInternal(Statement stmt, ParameterList paramValues,
		                                         IDbTransaction transaction) {
			ConfigStatement configStmt = stmt.GetConfigStatement(config);

			IDbCommand cmd = CreateCommandInternal(transaction);

			cmd.CommandText = configStmt.Code;
			cmd.CommandType =
				(CommandType)
				Enum.Parse(typeof (CommandType),
				           Enum.GetName(typeof (ConfigStatementType), configStmt.StatementType));

			AddParameters(configStmt, paramValues, cmd);

			return cmd;
		}

		private IDbCommand CreateSelectCommandInternal(DataTable table, string tableName,
		                                               ParameterList paramValues,
		                                               IDbTransaction transaction) {
			IDbCommand cmd = CreateCommandInternal(transaction);

			StringBuilder textBuilder = new StringBuilder(512);

			textBuilder.Append("SELECT ");

			int colCount = table.Columns.Count;
			if (colCount == 0) {
				textBuilder.Append("*");
			}
			else {
				bool first = true;
				for (int idx = 0; idx < colCount; idx++) {
					if (!first) {
						textBuilder.Append(", ");
					}
					DataColumn col = table.Columns[idx];
					textBuilder.Append(GetDbColumnName(col.ColumnName));
					first = false;
				}
			}
			textBuilder.Append(" ");

			textBuilder.Append("FROM ");
			textBuilder.Append(GetDbTableName(tableName));
			textBuilder.Append(" ");

			int paramCount = paramValues.Names.Count;
			if (paramCount > 0) {
				textBuilder.Append("WHERE ");

				bool first2 = true;
				foreach (String name in paramValues.Names) {
					if (!first2) {
						textBuilder.Append("AND ");
					}
					textBuilder.Append(string.Format("{0} = {1} ", GetDbColumnName(name), GetDbParameterName(name)));
					AddParameter(cmd, CreateDbParameter(table.Columns[name], paramValues.GetParameter(name).Value));
					first2 = false;
				}
			}

			cmd.CommandText = textBuilder.ToString();

			return cmd;
		}

		private IDbCommand CreateInsertCommandInternal(DataTable table, string tableName,
		                                               IDbTransaction transaction, bool refresh) {
			if (refresh && !SupportsRefreshAfterUpdate()) {
				throw new VanillaExecutionException("RefreshAfterUpdate not supported");
			}

			IDbCommand cmd = CreateCommandInternal(transaction);

			StringBuilder textBuilder = new StringBuilder(512);

			textBuilder.Append("INSERT INTO ");
			textBuilder.Append(GetDbTableName(tableName));
			textBuilder.Append(" ");

			textBuilder.Append("(");

			bool first = true;
			foreach (DataColumn col in table.Columns) {
				if (!col.AutoIncrement) {
					if (!first) {
						textBuilder.Append(",");
						textBuilder.Append(" ");
					}
					textBuilder.Append(col.ColumnName);
					first = false;
				}
			}
			textBuilder.Append(") ");

			textBuilder.Append("VALUES ");
			textBuilder.Append("(");
			first = true;
			foreach (DataColumn col in table.Columns) {
				if (!col.AutoIncrement) {
					if (!first) {
						textBuilder.Append(",");
						textBuilder.Append(" ");
					}
					textBuilder.Append(string.Format("{0}", GetDbParameterName(col.ColumnName)));
					AddParameter(cmd, col);
					first = false;
				}
			}
			textBuilder.Append(")");

			string identityParameterName = GetIdentityParameterName();

			DataColumn idCol = null;

			if (identityParameterName != null) {
				foreach (DataColumn keyCol in table.PrimaryKey) {
					if (keyCol.AutoIncrement) {
						idCol = keyCol;
						break;
					}
				}
			}

			if (idCol != null || refresh) {
				textBuilder.Append(";");
				textBuilder.Append("\n");
				textBuilder.Append("SELECT ");

				int colCount = table.Columns.Count;
				if (colCount == 0) {
					textBuilder.Append("*");
				}
				else {
					bool first2 = true;
					for (int idx = 0; idx < colCount; idx++) {
						if (!first2) {
							textBuilder.Append(", ");
						}
						DataColumn col = table.Columns[idx];
						textBuilder.Append(GetDbColumnName(col.ColumnName));
						first2 = false;
					}
				}
				textBuilder.Append(" ");

				textBuilder.Append("FROM ");
				textBuilder.Append(GetDbTableName(tableName));
				textBuilder.Append(" ");

				textBuilder.Append("WHERE ");

				if (idCol != null) {
					textBuilder.Append(
						string.Format("{0} = {1}", GetDbColumnName(idCol.ColumnName), identityParameterName));
				}
				else {
					bool first2 = true;
					foreach (DataColumn col in table.PrimaryKey) {
						if (!first2) {
							textBuilder.Append(" ");
							textBuilder.Append("AND ");
						}
						textBuilder.Append(
							string.Format("{0} = {1}", GetDbColumnName(col.ColumnName),
							              GetDbParameterName(col.ColumnName)));
						AddParameter(cmd, col);
						first2 = false;
					}
				}
			}

			cmd.CommandText = textBuilder.ToString();

			return cmd;
		}

		private void AddParameter(IDbCommand cmd, DataColumn col) 
		{
			AddParameter(cmd, CreateDbParameter(col));

		}

		private void AddParameter(IDbCommand cmd, IDbDataParameter param) 
		{
			if (RequiresDuplicateParameters() || !cmd.Parameters.Contains(param.ParameterName)) 
			{
				cmd.Parameters.Add(param);
			}

		}

		private IDbCommand CreateUpdateCommandInternal(DataTable table, string tableName,
		                                               IDbTransaction transaction, bool optLocking,
		                                               bool refresh) {
			if (refresh && !SupportsRefreshAfterUpdate()) {
				throw new VanillaExecutionException("RefreshAfterUpdate not supported");
			}

			if (table.PrimaryKey.Length == 0) {
				throw new VanillaExecutionException(
					string.Format("Primary key missing in datatable [{0}]", table.TableName));
			}

			IDbCommand cmd = CreateCommandInternal(transaction);

			StringBuilder textBuilder = new StringBuilder(512);

			ArrayList keys = new ArrayList();

			foreach (DataColumn col in table.PrimaryKey) {
				keys.Add(col);
			}

			textBuilder.Append("UPDATE ");
			textBuilder.Append(GetDbTableName(tableName));
			textBuilder.Append(" ");

			textBuilder.Append("SET ");
			bool first = true;
			foreach (DataColumn col in table.Columns) {
				if (!col.AutoIncrement && !keys.Contains(col)) {
					if (!first) {
						textBuilder.Append(",");
						textBuilder.Append(" ");
					}
					textBuilder.Append(
						string.Format("{0} = {1}", GetDbColumnName(col.ColumnName), GetDbParameterName(col.ColumnName)));
					AddParameter(cmd, col);
					first = false;
				}
			}

			textBuilder.Append(" ");
			textBuilder.Append("WHERE ");

			bool first2 = true;
			foreach (DataColumn col in keys) {
				if (!first2) {
					textBuilder.Append(" ");
					textBuilder.Append("AND ");
				}
				textBuilder.Append(
					string.Format("{0} = {1}", GetDbColumnName(col.ColumnName), GetDbParameterName(col.ColumnName)));
				AddParameter(cmd, col);
				first2 = false;
			}

			if (optLocking) {
				OptmisticLockingResult res = GetOptimisticLocking(table);
				textBuilder.Append(" ");
				textBuilder.Append(string.Format("AND ({0})", res.Clause));
				foreach (IDbDataParameter param in res.Parameters) {
					AddParameter(cmd, param);
				}
			}

			if (refresh) {
				textBuilder.Append(";");
				textBuilder.Append("\n");
				textBuilder.Append("SELECT ");

				int colCount = table.Columns.Count;
				if (colCount == 0) {
					textBuilder.Append("*");
				}
				else {
					bool first3 = true;
					for (int idx = 0; idx < colCount; idx++) {
						if (!first3) {
							textBuilder.Append(", ");
						}
						DataColumn col = table.Columns[idx];
						textBuilder.Append(GetDbColumnName(col.ColumnName));
						first3 = false;
					}
				}
				textBuilder.Append(" ");

				textBuilder.Append("FROM ");
				textBuilder.Append(GetDbTableName(tableName));
				textBuilder.Append(" ");

				textBuilder.Append("WHERE ");

				bool first4 = true;
				foreach (DataColumn col in table.PrimaryKey) {
					if (!first4) {
						textBuilder.Append(" ");
						textBuilder.Append("AND ");
					}
					textBuilder.Append(
						string.Format("{0} = {1}", GetDbColumnName(col.ColumnName), GetDbParameterName(col.ColumnName)));
					AddParameter(cmd, col);
					first4 = false;
				}
			}

			cmd.CommandText = textBuilder.ToString();


			return cmd;
		}

		private IDbCommand CreateDeleteCommandInternal(DataTable table, string tableName,
		                                               IDbTransaction transaction, bool optLocking) {
			if (table.PrimaryKey.Length == 0) {
				throw new VanillaExecutionException(
					string.Format("Primary key missing in datatable [{0}]", table.TableName));
			}

			IDbCommand cmd = CreateCommandInternal(transaction);

			StringBuilder textBuilder = new StringBuilder(512);

			textBuilder.Append("DELETE FROM ");
			textBuilder.Append(GetDbTableName(tableName));
			textBuilder.Append(" ");
			textBuilder.Append("WHERE ");

			bool first = true;
			foreach (DataColumn col in table.PrimaryKey) {
				if (!first) {
					textBuilder.Append(" ");
					textBuilder.Append("AND ");
				}
				textBuilder.Append(
					string.Format("{0} = {1}", GetDbColumnName(col.ColumnName), GetDbParameterName(col.ColumnName)));
				AddParameter(cmd, col);
				first = false;
			}

			if (optLocking) {
				OptmisticLockingResult res = GetOptimisticLocking(table);
				textBuilder.Append(string.Format("AND ({0})", res.Clause));
				foreach (IDbDataParameter param in res.Parameters) {
					AddParameter(cmd, param);
				}
			}

			cmd.CommandText = textBuilder.ToString();

			return cmd;
		}

		public class OptmisticLockingResult {
			private string clause;
			private ICollection parameters;

			public OptmisticLockingResult(string clauseParam, ICollection parametersParam) {
				clause = clauseParam;
				parameters = parametersParam;
			}

			public string Clause {
				get { return clause; }
			}

			public ICollection Parameters {
				get { return parameters; }
			}
		}

		private OptmisticLockingResult GetOptimisticLocking(DataTable table) {
			StringBuilder textBuilder = new StringBuilder(512);
			ArrayList keys = new ArrayList();
			ArrayList parameters = new ArrayList();

			foreach (DataColumn col in table.PrimaryKey) {
				keys.Add(col);
			}

			bool first = true;
			textBuilder.Append("(");
			foreach (DataColumn col in table.Columns) {
				if (!keys.Contains(col)) {
					if (!first) {
						textBuilder.Append(" AND ");
					}
					string colName = col.ColumnName;
					string paramName = string.Format("Original_{0}", col.ColumnName);
					string sqlColName = GetDbColumnName(colName);
					string sqlParamName = GetDbParameterName(paramName);
					textBuilder.Append(
						string.Format("({0} = {1} OR ({2} IS NULL AND {3} IS NULL))", sqlColName, sqlParamName,
						              sqlColName, sqlParamName));

					IDbDataParameter param1 = CreateDbParameter(col, null, colName);
					param1.SourceVersion = DataRowVersion.Original;
					parameters.Add(param1);

					IDbDataParameter param2 = CreateDbParameter(col, null, paramName);
					param2.SourceVersion = DataRowVersion.Original;
					parameters.Add(param2);

					first = false;
				}
			}

			textBuilder.Append(")");
			return new OptmisticLockingResult(textBuilder.ToString(), parameters);
		}


		public void Fill(FillParameter param) {
			IDbDataAdapter adpt = CreateDataAdapterInternal();
			if (param.StatementType == StatementType.ExplicitStatement) {
				adpt.SelectCommand = CreateCommandInternal(param.Statement, param.Parameters, param.Transaction);
			}
			else if (param.StatementType == StatementType.GenericStatement) {
				adpt.SelectCommand =
					CreateSelectCommandInternal(param.Table, param.TableName, param.Parameters, param.Transaction);
			}
			try {
				if (param.SchemaHandling == FillParameter.SchemaHandlingType.ApplyDBSchemaAlways ||
				    (param.SchemaHandling == FillParameter.SchemaHandlingType.ApplyDBSchemaOnEmptyDatasetSchema &&
				     param.Table.Columns.Count == 0)) {
					FillSchemaInternal(adpt, param.Table, SchemaType.Mapped);
				}
				if (config.LogSql) {
					Console.Out.WriteLine(adpt.SelectCommand.CommandText);
				}
				FillInternal(adpt, param.Table);
			}
			catch (VanillaException) {
				throw;
			}
			catch (Exception ex) {
				throw new VanillaExecutionException(ex.Message, ex);
			}
		}

		public int Update(UpdateParameter param) {
			IDbDataAdapter adpt = CreateDataAdapterInternal();
			adpt.InsertCommand =
				CreateInsertCommandInternal(param.Table, param.TableName, param.Transaction,
				                            param.RefreshAfterUpdate);
			adpt.UpdateCommand =
				CreateUpdateCommandInternal(param.Table, param.TableName, param.Transaction,
				                            param.Locking == UpdateParameter.LockingType.Optimistic,
				                            param.RefreshAfterUpdate);
			adpt.DeleteCommand =
				CreateDeleteCommandInternal(param.Table, param.TableName, param.Transaction,
				                            param.Locking == UpdateParameter.LockingType.Optimistic);
			try {
				if (config.LogSql) {
					if (param.Table.GetChanges(DataRowState.Added) != null) {
						Console.Out.WriteLine(adpt.InsertCommand.CommandText);
					}
					if (param.Table.GetChanges(DataRowState.Modified) != null) {
						Console.Out.WriteLine(adpt.UpdateCommand.CommandText);
					}
					if (param.Table.GetChanges(DataRowState.Deleted) != null) {
						Console.Out.WriteLine(adpt.DeleteCommand.CommandText);
					}
				}

				return UpdateInternal(adpt, param.Rows);
			}
			catch (VanillaException) {
				throw;
			}
			catch (DBConcurrencyException ex) {
				throw new VanillaConcurrencyException(ex.Message, ex);
			}
			catch (Exception ex) {
				throw new VanillaExecutionException(ex.Message, ex);
			}
		}

		public int ExecuteNonQuery(NonQueryParameter param) {
			IDbCommand cmd = CreateCommandInternal(param.Statement, param.Parameters, param.Transaction);

            //TODO: the same is ADO.NET default behavior for connection open/close, hence not necessary
            bool newConnection = cmd.Connection.State == ConnectionState.Closed;
			try {
				if (newConnection) {
					cmd.Connection.Open();
				}
				if (config.LogSql) {
					Console.Out.WriteLine(cmd.CommandText);
				}
				return cmd.ExecuteNonQuery();
			}
			catch (VanillaException) {
				throw;
			}
			catch (Exception ex) {
				throw new VanillaExecutionException(ex.Message, ex);
			}
			finally {
				if (newConnection) {
					cmd.Connection.Close();
				}
			}
		}

		public object ExecuteScalar(ScalarParameter param) {
			IDbCommand cmd = CreateCommandInternal(param.Statement, param.Parameters, param.Transaction);

			bool newConnection = cmd.Connection.State == ConnectionState.Closed;
			try {
                //TODO: the same is ADO.NET default behavior for connection open/close, hence not necessary
				if (newConnection) {
					cmd.Connection.Open();
				}
				if (config.LogSql) {
					Console.Out.WriteLine(cmd.CommandText);
				}
				return cmd.ExecuteScalar();
			}
			catch (VanillaException) {
				throw;
			}
			catch (Exception ex) {
				throw new VanillaExecutionException(ex.Message, ex);
			}
			finally {
				if (newConnection) {
					cmd.Connection.Close();
				}
			}
		}

        public void ExecuteInTransaction(TransactionCallback cb, object obj) {
            IDbConnection conn = null;
            IDbTransaction tx = null;
            try {
                if (TransactionScope.GetCurrentTransaction() == null) {
                    conn = CreateConnection();
                    conn.Open();
                    tx = conn.BeginTransaction();
                    TransactionScope.SetCurrentTransaction(tx);
                }

                cb(this, obj);

                if (tx != null) {
                    tx.Commit();
                }
            }
            catch (Exception ex) {
                if (tx != null) {
                    tx.Rollback();
                }
                throw new VanillaExecutionException(ex.Message, ex);
            }
            finally {
                if (tx != null) {
                    TransactionScope.SetCurrentTransaction(null);
                }
                if (conn != null) {
                    if (conn != null && conn.State != ConnectionState.Closed) {
                        conn.Close();
                    }
                }
            }
        }

	}
}