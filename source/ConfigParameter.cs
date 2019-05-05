using System.Xml.Serialization;

namespace VanillaDAL.Config {
	public enum ConfigParameterType {
		Undefined,
		Byte,
		Int16,
		Int32,
		Int64,
		Double,
		Boolean,
		DateTime,
		String,
		Guid,
		Decimal,
		ByteArray,
        Variant
	}

	[XmlRoot(ElementName = "Parameter")]
	public class ConfigParameter {
		private string name;

		public string Name {
			get {
				if (name == null) {
					throw new VanillaConfigException("[Parameter.Name] has not been set");
				}
				return name;
			}
			set { name = value; }
		}

		private ConfigParameterType type;

		public ConfigParameterType Type {
			get {
				if (type == ConfigParameterType.Undefined) {
					throw new VanillaConfigException("[Parameter.Type] has not been set");
				}
				return type;
			}
			set { type = value; }
		}
	}
}