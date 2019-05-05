using System;
using System.Collections;
using System.Data;
using System.Data.OleDb;
using VanillaDAL.Config;

namespace VanillaDAL {

	internal class DBAccessorOLEDB : DBAccessorBase {

		private static DictionaryEntry[] XML_TO_OLEDB_LIST = new DictionaryEntry[] {
			new DictionaryEntry(
				ConfigParameterType.Boolean,
				OleDbType.Boolean),
			new DictionaryEntry(
				ConfigParameterType.Byte, OleDbType.TinyInt)
			,
			new DictionaryEntry(
				ConfigParameterType.ByteArray,
				OleDbType.Binary),
			new DictionaryEntry(
				ConfigParameterType.DateTime, OleDbType.Date)
			,
			new DictionaryEntry(
				ConfigParameterType.Decimal,
				OleDbType.Decimal),
			new DictionaryEntry(
				ConfigParameterType.Double, OleDbType.Double)
			,
			new DictionaryEntry(
				ConfigParameterType.Guid, OleDbType.Guid),
			new DictionaryEntry(
				ConfigParameterType.Int16,
				OleDbType.SmallInt),
			new DictionaryEntry(
				ConfigParameterType.Int32, OleDbType.Integer)
			,
			new DictionaryEntry(
				ConfigParameterType.Int64, OleDbType.BigInt)
			,
			new DictionaryEntry(
				ConfigParameterType.String,
				OleDbType.VarWChar)
           ,
           new DictionaryEntry(
               ConfigParameterType.Variant,
               OleDbType.Variant)
        };

		private static Hashtable XML_TO_OLEDB_MAP = new Hashtable();

		static DBAccessorOLEDB() {
			foreach (DictionaryEntry entry in XML_TO_OLEDB_LIST) {
				XML_TO_OLEDB_MAP[entry.Key] = entry.Value;
			}
		}

		internal DBAccessorOLEDB(VanillaConfig _config) : base(_config) {
		}

		private OleDbType GetOleDbType(ConfigParameterType type) {
			if (XML_TO_OLEDB_MAP.ContainsKey(type)) {
				return (OleDbType) XML_TO_OLEDB_MAP[type];
			}
			return OleDbType.Variant;
		}

		protected override IDbDataAdapter CreateDataAdapterInternal() {
			return new OleDbDataAdapter();
		}

		protected override IDbConnection CreateConnectionInternal() {
			return new OleDbConnection(config.ConnectionString);
		}

		protected override IDbCommand CreateCommandInternal() {
			return new OleDbCommand();
		}

		protected override IDbDataParameter CreateDbParameter(Parameter param,
		                                                      ConfigParameterType configParamType) {
			OleDbParameter dbParam =
				new OleDbParameter(GetDbParameterName(param.Name), GetOleDbType(configParamType),
				                   configParamType == ConfigParameterType.String && param.Value is string
				                   	? param.Value.ToString().Length
				                   	: 0,
				                   param.Name);
			dbParam.Value = param.Value;
			return dbParam;
		}

		protected override IDbDataParameter CreateDbParameter(DataColumn col, object val, string paramName) {
			OleDbParameter dbParam =
				new OleDbParameter(GetDbParameterName(paramName),
				                   GetOleDbType(GetConfigParameterType(col.DataType)), 0,
				                   col.ColumnName);
			if (val != null) {
				dbParam.Value = val;
			}
			return dbParam;
		}

		protected override string GetDbParameterName(string name) {
			return string.Format("@{0}", name);
		}

		protected override int UpdateInternal(IDbDataAdapter adpt, DataRow[] rows) {
			return ((OleDbDataAdapter) adpt).Update(rows);
		}

		protected override void FillInternal(IDbDataAdapter adpt, DataTable table) {
			((OleDbDataAdapter) adpt).Fill(table);
		}

		protected override void FillSchemaInternal(IDbDataAdapter adpt, DataTable table, SchemaType type) {
			((OleDbDataAdapter) adpt).FillSchema(table, type);
		}

		protected override bool RequiresDuplicateParameters() 
		{
			return true;
		}
	}
}