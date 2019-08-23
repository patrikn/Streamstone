﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Custom_event_properties
    {
        [Test]
        public void When_passing_property_with_reserved_name()
        {
            var reserved = ReservedEventProperties()
                .ToDictionary(p => p, p => new EntityProperty(42));

            var properties = EventProperties.From(reserved);

            Assert.That(properties.Count, Is.EqualTo(0),
                "Should skip all properties with reserved names, such as RowKey, Id, etc");
        }

        static IEnumerable<string> ReservedEventProperties()
        {
            return typeof(EventEntity)
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(p => !p.GetCustomAttributes<IgnorePropertyAttribute>(true).Any())
                    .Where(p => p.Name != "Properties")
                    .Where(p => p.Name != "EventProperties")
                    .Select(p => p.Name);
        }
    }
}