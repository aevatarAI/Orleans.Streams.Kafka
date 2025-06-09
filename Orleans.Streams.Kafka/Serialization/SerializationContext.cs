using Orleans.Serialization;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		// Stage 1: Simple property type change
		public Orleans.Serialization.Serializer SerializationManager { get; set; }
		public IExternalStreamDeserializer ExternalStreamDeserializer { get; set; }
	}
}
