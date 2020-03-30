using System;
using System.Threading;
using Confluent.Kafka;

namespace ProcessNewOrder
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "new-pet-group-3",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build()){
                consumer.Subscribe("PETSHOP_NEW_ORDER");
                var cts = new CancellationTokenSource();

                 try
                {
                    while (true)
                    {
                        var message = consumer.Consume(cts.Token);
                        Console.WriteLine("------------------------------------------------");
                        Console.WriteLine($"Mensagem: {message.Value} recebida de {message.TopicPartitionOffset}");
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
