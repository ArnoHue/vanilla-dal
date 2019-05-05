using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using VanillaDAL.Config;

namespace VanillaDAL {

	internal class DBAccessorSQLServer : DBAccessorBase {

		private static DictionaryEntry[] XML_TO_SQLSERVER_LIST = new DictionaryEntry[] {
			new
				DictionaryEntry(
				ConfigParameterType.Boolean,
				SqlDbType.Bit),
			new
				DictionaryEntry(
				ConfigParameterType.Byte,
				SqlDbType.Char)
			,
			new
				DictionaryEntry(
				ConfigParameterType.ByteArray,
				SqlDbType.Image),
			new
				DictionaryEntry(
				ConfigParameterType.DateTime,
				SqlDbType.DateTime),
			new
				DictionaryEntry(
				ConfigParameterType.Decimal,
				SqlDbType.Decimal)
			,
			new
				DictionaryEntry(
				ConfigParameterType.Double,
				SqlDbType.Float),
			new
				DictionaryEntry(
				ConfigParameterType.Guid,
				SqlDbType.UniqueIdentifier)
			,
			new
				DictionaryEntry(
				ConfigParameterType.Int16,
				SqlDbType.SmallInt),
			new
				DictionaryEntry(
				ConfigParameterType.Int32,
				SqlDbType.Int)
			,
			new
				DictionaryEntry(
				ConfigParameterType.Int64,
				SqlDbType.BigInt)
			,
			new
				DictionaryEntry(
				ConfigParameterType.String,
				SqlDbType.NVarChar)
            ,
            new
                DictionaryEntry(
                ConfigParameterType.Variant,
                SqlDbType.Variant)
        };

		private static Hashtable XML_TO_SQLSERVER_MAP = new Hashtable();

		static DBAccessorSQLServer() {
			foreach (DictionaryEntry entry in XML_TO_SQLSERVER_LIST) {
				XML_TO_SQLSERVER_MAP[entry.Key] = entry.Value;
			}
		}

		internal DBAccessorSQLServer(VanillaConfig _config) : base(_config) {
		}

		private SqlDbType GetSqlDbType(ConfigParameterType type) {
			return (SqlDbType) XML_TO_SQLSERVER_MAP[type];
		}

		protected override IDbDataAdapter CreateDataAdapterInternal() {
			return new SqlDataAdapter();
		}

		protected override IDbConnection CreateConnectionInternal() {
			return new SqlConnection(config.ConnectionString);
		}

		protected override IDbCommand CreateCommandInternal() {
			return new SqlCommand();
		}

		protected override IDbDataParameter CreateDbParameter(Parameter param,
		                                                      ConfigParameterType configParamType) {
			SqlParameter dbParam =
				new SqlParameter(GetDbParameterName(param.Name), GetSqlDbType(configParamType),
				                 configParamType == ConfigParameterType.String && param.Value is string
				                 	? param.Value.ToString().Length
				                 	: 0,
				                 param.Name);
			dbParam.Value = param.Value;
			return dbParam;
		}

		protected override IDbDataParameter CreateDbParameter(DataColumn col, object val, string paramName) {
			SqlParameter dbParam =
				new SqlParameter(GetDbParameterName(paramName),
				                 GetSqlDbType(GetConfigParameterType(col.DataType)),
				                 0,
				                 col.ColumnName);
			if (val != null) {
				dbParam.Value = val;
			}
			return dbParam;
		}

		protected override string GetDbParameterName(string name) {
			return string.Format("@{0}", name);
		}

		protected override string GetDbColumnName(string name) {
			return string.Format("[{0}]", name);
		}

		protected override string GetDbTableName(string name) {
			return string.Format("[{0}]", name);
		}

		protected override int UpdateInternal(IDbDataAdapter adpt, DataRow[] rows) {
			return ((SqlDataAdapter) adpt).Update(rows);
		}

		protected override void FillInternal(IDbDataAdapter adpt, DataTable table) {
			((SqlDataAdapter) adpt).Fill(table);
		}

		protected override void FillSchemaInternal(IDbDataAdapter adpt, DataTable table, SchemaType type) {
			((SqlDataAdapter) adpt).FillSchema(table, type);
		}

		protected override string GetIdentityParameterName() {
			return "@@IDENTITY";
		}

		protected override bool SupportsRefreshAfterUpdate() {
			return true;
		}
	}
}