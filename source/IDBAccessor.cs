using System;
using System.Data;

namespace VanillaDAL 
{
	public enum StatementType {
		GenericStatement,
		ExplicitStatement
	}

	public interface ITransactionTask {
		void Execute(IDBAccessor accessor, IDbTransaction trans);
	}

	public class FillTransactionTask : ITransactionTask {
		private FillParameter fillParam;

		public FillTransactionTask(FillParameter _fillParam) {
			if (_fillParam.Transaction != null) {
				throw new VanillaExecutionException("Transaction already set on FillParameter");
			}
			fillParam = _fillParam;
		}

		public void Execute(IDBAccessor accessor, IDbTransaction trans) {
			try {
				fillParam.Transaction = trans;
				accessor.Fill(fillParam);
			}
			finally {
				fillParam.Transaction = null;
			}
		}
	}

	public class UpdateTransactionTask : ITransactionTask {
		private UpdateParameter updateParam;

		public UpdateTransactionTask(UpdateParameter _updateParam) {
			if (_updateParam.Transaction != null) {
				throw new VanillaExecutionException("Transaction already set on UpdateParameter");
			}
			updateParam = _updateParam;
		}

		public void Execute(IDBAccessor accessor, IDbTransaction trans) {
			try {
				updateParam.Transaction = trans;
				accessor.Update(updateParam);
			}
			finally {
				updateParam.Transaction = null;
			}
		}
	}

	public class ExecuteNonQueryTransactionTask : ITransactionTask {
		private NonQueryParameter nonQueryParam;

		public ExecuteNonQueryTransactionTask(NonQueryParameter _nonQueryParam) {
			if (_nonQueryParam.Transaction != null) {
				throw new VanillaExecutionException("Transaction already set on NonQueryParameter");
			}
			nonQueryParam = _nonQueryParam;
		}

		public void Execute(IDBAccessor accessor, IDbTransaction trans) {
			try {
				nonQueryParam.Transaction = trans;
				accessor.ExecuteNonQuery(nonQueryParam);
			}
			finally {
				nonQueryParam.Transaction = null;
			}
		}
	}

	public class ExecuteScalarTransactionTask : ITransactionTask {
		private ScalarParameter scalarParam;

		public ExecuteScalarTransactionTask(ScalarParameter _scalarParam) {
			if (_scalarParam.Transaction != null) {
				throw new VanillaExecutionException("Transaction already set on ScalarParameter");
			}
			scalarParam = _scalarParam;
		}

		public void Execute(IDBAccessor accessor, IDbTransaction trans) {
			try {
				scalarParam.Transaction = trans;
				accessor.ExecuteScalar(scalarParam);
			}
			finally {
				scalarParam.Transaction = null;
			}
		}
	}

	public class FillParameter : AbstractParameter {
		private SchemaHandlingType schemaHandling = SchemaHandlingType.ApplyDBSchemaOnEmptyDatasetSchema;

		public enum SchemaHandlingType {
			ApplyDBSchemaOnEmptyDatasetSchema,
			ApplyDBSchemaAlways,
			IgnoreDBSchema
		}

		public FillParameter(DataTable _table, Statement _statement)
			: base(_table, _statement) {
		}

		public FillParameter(DataTable _table, Statement _statement, ParameterList _parameters)
			: base(_table, _statement, _parameters) {
		}

		public FillParameter(DataTable _table, Statement _statement, ParameterList _parameters,
		                     IDbTransaction _trans)
			: base(_table, _statement, _parameters, _trans) {
		}

		public FillParameter(DataTable _table)
			: base(_table) {
		}

		public FillParameter(DataTable _table, string _tableName)
			: base(_table, _tableName) {
		}

		public FillParameter(DataTable _table, ParameterList _parameters)
			: base(_table, _parameters) {
		}

		public FillParameter(DataTable _table, string _tableName, ParameterList _parameters)
			: base(_table, _tableName, _parameters) {
		}

		public FillParameter(DataTable _table, string _tableName, ParameterList _parameters,
		                     IDbTransaction _trans)
			: base(_table, _tableName, _parameters, _trans) {
		}

		public SchemaHandlingType SchemaHandling {
			get { return schemaHandling; }
			set { schemaHandling = value; }
		}
	}

	public class UpdateParameter : AbstractParameter {
		private LockingType locking = LockingType.None;
		private bool refreshAfterUpdate = false;
        private DataRow[] rows = null;

		public enum LockingType {
			None,
			Optimistic
		}

		public UpdateParameter(DataTable _table)
			: this(_table, _table.TableName) {
		}

		public UpdateParameter(DataTable _table, string _tableName)
			: this(_table, _tableName, null) {
		}

		public UpdateParameter(DataTable _table, string _tableName, IDbTransaction _trans)
			: base(_table, _tableName, _trans) {
            rows = new DataRow[Table.Rows.Count];
            Table.Rows.CopyTo(rows, 0);
        }

        public UpdateParameter(DataRow _row)
            : this(new DataRow[] { _row } , _row.Table.TableName, null) {
        }

        public UpdateParameter(DataRow _row, string _tableName)
            : this(new DataRow[] { _row } ,_tableName, null) {
        }

        public UpdateParameter(DataRow _row, string _tableName, IDbTransaction _trans)
            : this(new DataRow[] { _row } ,_tableName, _trans) {
        }

        public UpdateParameter(DataRow[] _rows, string _tableName, IDbTransaction _trans)
            : base(_rows[0].Table, _tableName, _trans) {
            rows = _rows;
        }

        public DataRow[] Rows {
            get {
                return rows;
            }
        }

        public LockingType Locking {
            get { return locking; }
            set { locking = value; }
		}

		public bool RefreshAfterUpdate {
			get { return refreshAfterUpdate; }
			set { refreshAfterUpdate = value; }
		}
	}

	public class NonQueryParameter : AbstractParameter {
		public NonQueryParameter(Statement _statement)
			: base(_statement) {
		}

		public NonQueryParameter(Statement _statement, ParameterList _parameters)
			: base(_statement, _parameters) {
		}

		public NonQueryParameter(Statement _statement, ParameterList _parameters, IDbTransaction _trans)
			: base(_statement, _parameters, _trans) {
		}
	}

	public class ScalarParameter : AbstractParameter {
		public ScalarParameter(Statement _statement)
			: base(_statement) {
		}

		public ScalarParameter(Statement _statement, ParameterList _parameters)
			: base(_statement, _parameters) {
		}

		public ScalarParameter(Statement _statement, ParameterList _parameters, IDbTransaction _trans)
			: base(_statement, _parameters, _trans) {
		}
	}

	public abstract class AbstractParameter {
		private DataTable table;
		private Statement statement;
		private string tableName;
		private ParameterList parameters;
		private IDbTransaction transaction;
		private StatementType statementType;

		public StatementType StatementType {
			get { return statementType; }
		}


		public IDbTransaction Transaction {
			get { return transaction; }
			set { transaction = value; }
		}


		public ParameterList Parameters {
			get { return parameters; }
		}

		public string TableName {
			get { return tableName; }
		}

		public Statement Statement {
			get { return statement; }
		}

		public DataTable Table {
			get { return table; }
		}

		protected AbstractParameter(Statement _statement)
			: this(null, _statement, null, null) {
		}

		protected AbstractParameter(Statement _statement, ParameterList _parameters)
			: this(null, _statement, _parameters, null) {
		}

		protected AbstractParameter(Statement _statement, ParameterList _parameters, IDbTransaction _trans)
			: this(null, _statement, _parameters, _trans) {
		}

		protected AbstractParameter(DataTable _table, Statement _statement)
			: this(_table, _statement, null, null) {
		}

		protected AbstractParameter(DataTable _table, Statement _statement, ParameterList _parameters)
			: this(_table, _statement, _parameters, null) {
		}

		protected AbstractParameter(DataTable _table, Statement _statement, ParameterList _parameters,
		                            IDbTransaction _trans) {
			table = _table;
			statement = _statement;
            if (statement.IsParameterInjectionRequired()) {
                statement.InjectParameters(_parameters);
            }
			parameters = _parameters != null ? _parameters : new ParameterList();
			transaction = _trans;
			statementType = StatementType.ExplicitStatement;
		}

		protected AbstractParameter(DataTable _table)
			: this(_table, _table.TableName, null, null) {
		}

		protected AbstractParameter(DataTable _table, string _tableName)
			: this(_table, _tableName, null, null) {
		}

		protected AbstractParameter(DataTable _table, string _tableName, IDbTransaction _trans)
			: this(_table, _tableName, null, _trans) {
		}

		protected AbstractParameter(DataTable _table, ParameterList _parameters)
			: this(_table, _table.TableName, _parameters, null) {
		}

		protected AbstractParameter(DataTable _table, string _tableName, ParameterList _parameters)
			: this(_table, _tableName, _parameters, null) {
		}

		protected AbstractParameter(DataTable _table, string _tableName, ParameterList _parameters,
		                            IDbTransaction _trans) {
			table = _table;
			tableName = _tableName;
			parameters = _parameters != null ? _parameters : new ParameterList();
			transaction = _trans;
			statementType = StatementType.GenericStatement;
		}
	}

	public abstract class CommandParameter {
		private Statement statement;
		private ParameterList parameters;
		private IDbTransaction transaction;

		public IDbTransaction Transaction {
			get { return transaction; }
		}

		public ParameterList Parameters {
			get { return parameters; }
		}

		public Statement Statement {
			get { return statement; }
		}


		protected CommandParameter()
			: this(null, null, null) {
		}

		protected CommandParameter(Statement _statement, ParameterList _parameters)
			: this(_statement, _parameters, null) {
		}

		protected CommandParameter(Statement _statement, ParameterList _parameters, IDbTransaction _trans) {
			statement = _statement;
			parameters = _parameters;
			transaction = _trans;
		}
	}

    public delegate void TransactionCallback(IDBAccessor acc, object obj);

	public interface IDBAccessor {
		IDbCommand CreateCommand(CommandParameter param);
		IDbConnection CreateConnection();
		IDbDataAdapter CreateDataAdapter();

		void Fill(FillParameter param);
		int Update(UpdateParameter param);
		int ExecuteNonQuery(NonQueryParameter param);
		object ExecuteScalar(ScalarParameter param);
        void ExecuteInTransaction(TransactionCallback callback, object obj);
	}
}