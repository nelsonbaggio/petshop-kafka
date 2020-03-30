using System;
using System.Threading;
using Confluent.Kafka;

namespace SendEmail
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "new-pet-group-4",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build()){
                consumer.Subscribe("PETSHOP_SEND_EMAIL");
                var cts = new CancellationTokenSource();

                 try
                {
                    while (true)
                    {
                        var message = consumer.Consume(cts.Token);
                        Console.WriteLine("------------------------------------------------");
                        Console.WriteLine($"Email enviado: {message.Value} [{message.TopicPartitionOffset}]");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}
