using System;
using System.Collections;
using System.Data;
using System.Data.OracleClient;
using VanillaDAL.Config;

namespace VanillaDAL {
	internal class DBAccessorOracle : DBAccessorBase {
		private static DictionaryEntry[] XML_TO_ORACLE_LIST = new DictionaryEntry[] {
			new DictionaryEntry(
				ConfigParameterType.Boolean,
				OracleType.Byte),
			new DictionaryEntry(
				ConfigParameterType.Byte, OracleType.Byte),
			new DictionaryEntry(
				ConfigParameterType.ByteArray,
				OracleType.Raw),
			new DictionaryEntry(
				ConfigParameterType.DateTime,
				OracleType.DateTime),
			new DictionaryEntry(
				ConfigParameterType.Decimal,
				OracleType.Number),
			new DictionaryEntry(
				ConfigParameterType.Double,
				OracleType.Double),
			new DictionaryEntry(
				ConfigParameterType.Guid, OracleType.Raw),
			new DictionaryEntry(
				ConfigParameterType.Int16, OracleType.Int16)
			,
			new DictionaryEntry(
				ConfigParameterType.Int32, OracleType.Int32)
			,
			new DictionaryEntry(
				ConfigParameterType.Int64,
				OracleType.Number),
			new DictionaryEntry(
				ConfigParameterType.String,
				OracleType.NVarChar),
			new DictionaryEntry(
				ConfigParameterType.String,
				OracleType.NVarChar),
            new DictionaryEntry(
                ConfigParameterType.Variant,
                OracleType.NVarChar)
        };

		private static Hashtable XML_TO_ORACLE_MAP = new Hashtable();

		static DBAccessorOracle() {
			foreach (DictionaryEntry entry in XML_TO_ORACLE_LIST) {
				XML_TO_ORACLE_MAP[entry.Key] = entry.Value;
			}
		}

		internal DBAccessorOracle(VanillaConfig _config) : base(_config) {
		}

		private OracleType GetOracleType(ConfigParameterType type) {
			return (OracleType) XML_TO_ORACLE_MAP[type];
		}

		protected override IDbDataAdapter CreateDataAdapterInternal() {
			return new OracleDataAdapter();
		}

		protected override IDbConnection CreateConnectionInternal() {
			return new OracleConnection(config.ConnectionString);
		}

		protected override IDbCommand CreateCommandInternal() {
			return new OracleCommand();
		}

		protected override IDbDataParameter CreateDbParameter(Parameter param,
		                                                      ConfigParameterType configParamType) {
			OracleParameter dbParam =
				new OracleParameter(GetDbParameterName(param.Name), GetOracleType(configParamType),
				                    configParamType == ConfigParameterType.String && param.Value is string
				                    	? param.Value.ToString().Length
				                    	: 0,
				                    param.Name);
			dbParam.Value = param.Value;
			return dbParam;
		}

		protected override IDbDataParameter CreateDbParameter(DataColumn col, object val, string paramName) {
			OracleParameter dbParam =
				new OracleParameter(GetDbParameterName(paramName),
				                    GetOracleType(GetConfigParameterType(col.DataType)), 0,
				                    col.ColumnName);
			if (val != null) {
				dbParam.Value = val;
			}
			return dbParam;
		}

		protected override string GetDbParameterName(string name) {
			return string.Format(":{0}", name);
		}

		protected override int UpdateInternal(IDbDataAdapter adpt, DataRow[] rows) {
			return ((OracleDataAdapter) adpt).Update(rows);
		}

		protected override void FillInternal(IDbDataAdapter adpt, DataTable table) {
			((OracleDataAdapter) adpt).Fill(table);
		}

		protected override void FillSchemaInternal(IDbDataAdapter adpt, DataTable table, SchemaType type) {
			((OracleDataAdapter) adpt).FillSchema(table, type);
		}
	}
}