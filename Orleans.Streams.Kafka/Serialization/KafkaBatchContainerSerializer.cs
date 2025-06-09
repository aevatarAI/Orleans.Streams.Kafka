using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using System.Buffers;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class KafkaBatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly Orleans.Serialization.Serializer _serializer;

		public KafkaBatchContainerSerializer(Orleans.Serialization.Serializer serializer)
		{
			_serializer = serializer;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
		{
			var buffer = new ArrayBufferWriter<byte>();
			_serializer.Serialize(data, buffer);
			return buffer.WrittenMemory.ToArray();
		}
	}
}