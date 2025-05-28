using Orleans.Runtime;
using Orleans.Streams.Kafka.Core;
using System;
using System.Collections.Generic;
using Xunit;

namespace Orleans.Streams.Kafka.E2E.Tests
{
    public class KafkaBatchContainerTests
    {
        [Fact]
        public void Events_Should_Be_Defensive_Copy()
        {
            // Arrange
            var originalEvents = new List<object> { "a", "b", "c" };
            var container = new KafkaBatchContainer(
                streamId: new StreamId(),
                events: originalEvents,
                requestContext: null
            );

            // Act
            originalEvents.Add("d");

            // Assert
            Assert.Equal(3, container.Events.Count);
            Assert.DoesNotContain("d", container.Events);
        }
    }
} 