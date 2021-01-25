using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Confluent.Kafka;
using Dfe.Edis.Kafka.SchemaRegistry;
using Dfe.Edis.Kafka.Serialization;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using NJsonSchema;
using NJsonSchema.Generation;
using NUnit.Framework;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Dfe.Edis.Kafka.UnitTests.SerializationTests.KafkaJsonSerializerTests
{
    public class WhenSerializingAsync
    {
        private static readonly string DefaultBasicObjectSchemaJson = JsonSchema.FromType<BasicObject>(new JsonSchemaGeneratorSettings
        {
            SerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
            },
        }).ToJson();

        private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };

        [Test, AutoData]
        public async Task ThenItShouldSerializeAStringCorrectly(string data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<string>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldSerializeAnIntegerCorrectly(int data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<int>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldSerializeALongCorrectly(long data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<long>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldSerializeADateTimeCorrectly(DateTime data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<DateTime>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
            Assert.AreEqual(expected, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldSerializeAnObjectCorrectly(BasicObject data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, new JsonSerializerOptions {PropertyNamingPolicy = JsonNamingPolicy.CamelCase}));
            Assert.AreEqual(expected, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldGetSchemaFromRegistry(BasicObject data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            schemaRegistryClientMock.Setup(c => c.ListSchemaVersionsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] {1, 2, 3});
            schemaRegistryClientMock.Setup(c => c.GetSchemaAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new SchemaDetails
                {
                    Id = 123,
                    Version = 3,
                    SchemaType = "JSON",
                    Subject = "some-test-subject",
                    Schema = DefaultBasicObjectSchemaJson,
                });
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            await serializer.SerializeAsync(data, GetContext());

            const string expectedSubjectName = "test-topic-value";
            schemaRegistryClientMock.Verify(c => c.ListSchemaVersionsAsync(expectedSubjectName, It.IsAny<CancellationToken>()),
                Times.Once);
            schemaRegistryClientMock.Verify(c => c.GetSchemaAsync(expectedSubjectName, 3, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Test, AutoData]
        public async Task ThenItShouldNotGetSchemaFromRegistryIfNoVersions(BasicObject data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            await serializer.SerializeAsync(data, GetContext());

            const string expectedSubjectName = "test-topic-value";
            schemaRegistryClientMock.Verify(c => c.ListSchemaVersionsAsync(expectedSubjectName, It.IsAny<CancellationToken>()),
                Times.Once);
            schemaRegistryClientMock.Verify(c => c.GetSchemaAsync(expectedSubjectName, 3, It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Test, AutoData]
        public void ThenItShouldThrowExceptionIfSchemaIsNotJson(BasicObject data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            schemaRegistryClientMock.Setup(c => c.ListSchemaVersionsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] {1, 2, 3});
            schemaRegistryClientMock.Setup(c => c.GetSchemaAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new SchemaDetails
                {
                    Id = 123,
                    Version = 3,
                    SchemaType = "AVRO",
                    Subject = "some-test-subject",
                });
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = Assert.ThrowsAsync<KafkaSerializationException>(async () =>
                await serializer.SerializeAsync(data, GetContext()));
            Assert.AreEqual("Unable to verify schema for subject test-topic-value, version 3, as the schema is AVRO but expected JSON", actual.Message);
        }

        [Test, AutoData]
        public async Task ThenItShouldSerializeReturnSerializedObjectIfValidForSchema(BasicObject data)
        {
            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            schemaRegistryClientMock.Setup(c => c.ListSchemaVersionsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] {1, 2, 3});
            schemaRegistryClientMock.Setup(c => c.GetSchemaAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new SchemaDetails
                {
                    Id = 123,
                    Version = 3,
                    SchemaType = "JSON",
                    Subject = "some-test-subject",
                    Schema = DefaultBasicObjectSchemaJson,
                });
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = await serializer.SerializeAsync(data, GetContext());

            var expected = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data,
                new JsonSerializerOptions {PropertyNamingPolicy = JsonNamingPolicy.CamelCase}));
            Assert.AreEqual(expected, actual);
        }

        [Test]
        public void ThenItShouldThrowExceptionIfDataNotValidForSchema()
        {
            var jsonSchema = @"{
                ""$schema"": ""http://json-schema.org/draft-04/schema#"",
                ""title"": ""BasicObject"",
                ""type"": ""object"",
                ""additionalProperties"": false,
                ""properties"": {
                    ""name"": {
                        ""type"": ""boolean""
                    },
                    ""timeStamp"": {
                        ""type"": ""string"",
                        ""format"": ""date-time""
                    }
                }
            }";
            var data = new BasicObject
            {
                Id = 99,
                Name = "test 123",
                TimeStamp = new DateTime(2021, 1, 25, 10, 17, 32),
            };

            var schemaRegistryClientMock = GetSchemaRegistryClientMock();
            schemaRegistryClientMock.Setup(c => c.ListSchemaVersionsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] {1, 2, 3});
            schemaRegistryClientMock.Setup(c => c.GetSchemaAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new SchemaDetails
                {
                    Id = 123,
                    Version = 3,
                    SchemaType = "JSON",
                    Subject = "some-test-subject",
                    Schema = jsonSchema,
                });
            var serializer = new KafkaJsonSerializer<BasicObject>(schemaRegistryClientMock.Object, DefaultJsonSerializerOptions);

            var actual = Assert.ThrowsAsync<KafkaJsonSchemaSerializationException>(async () =>
                await serializer.SerializeAsync(data, GetContext()));
            Assert.AreEqual("Value did not conform to schema. 2 errors found (See ValidationErrors for more details)", actual.Message);
            Assert.IsNotNull(actual.ValidationErrors);
            Assert.AreEqual(2, actual.ValidationErrors.Length);
        }


        private SerializationContext GetContext()
        {
            return new SerializationContext(MessageComponentType.Value, "test-topic", null);
        }

        private Mock<ISchemaRegistryClient> GetSchemaRegistryClientMock()
        {
            var schemaRegistryClientMock = new Mock<ISchemaRegistryClient>();

            schemaRegistryClientMock.Setup(c => c.ListSchemaVersionsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new int[0]);

            return schemaRegistryClientMock;
        }
    }
}