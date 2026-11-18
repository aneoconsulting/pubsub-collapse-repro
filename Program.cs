// Reproducer: a Pub/Sub subscription whose unary Pull stops delivering (empty Pulls of ~18 s while the backlog
// still holds hundreds of thousands of messages).
//
//   dotnet run -c Release -- repro   <project> [options]   fresh topic + subscription, three drains, verdict
//   dotnet run -c Release -- publish <project> <topic> <count>
//   dotnet run -c Release -- drain   <project> <subscription> <expected> [options]
//
// Options: --consumers 3000  --messages 500000  --hold-ms 200  --modack  --seconds 360  --keep
//
// A consumer is one SubscriberServiceApiClient (its own gRPC channel) looping on: Pull(maxMessages 1), optional
// ModifyAckDeadline(120 s), wait hold-ms (the task), Acknowledge. This is the loop of an ArmoniK Core agent.

using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Auth;
using Grpc.Core;

ThreadPool.SetMinThreads(256, 256);

if (args.Length < 2)
{
  Console.Error.WriteLine("usage: repro <project> | publish <project> <topic> <count> | drain <project> <subscription> <expected> [options]");
  return 2;
}

var command = args[0];
var project = args[1];
var positional = args.Skip(2).TakeWhile(a => !a.StartsWith("--")).ToArray();
var options = args.Skip(2 + positional.Length).ToArray();
string Opt(string name, string fallback)
{
  var i = Array.IndexOf(options, "--" + name);
  return i >= 0 && i + 1 < options.Length && !options[i + 1].StartsWith("--") ? options[i + 1] : fallback;
}
bool Flag(string name) => options.Contains("--" + name);

var consumers = int.Parse(Opt("consumers", "3000"));
var messages = int.Parse(Opt("messages", "500000"));
var seconds = int.Parse(Opt("seconds", "360"));

var credential = (await GoogleCredential.GetApplicationDefaultAsync()).CreateScoped(SubscriberServiceApiClient.DefaultScopes);
var channelCredentials = credential.ToChannelCredentials();
SubscriberServiceApiClient[]? consumerClients = null;

switch (command)
{
  case "publish":
    await Publish(TopicName.FromProjectTopic(project, positional[0]), int.Parse(positional[1]));
    return 0;
  case "drain":
    var r = await Drain(SubscriptionName.FromProjectSubscription(project, positional[0]), int.Parse(positional[1]),
                        int.Parse(Opt("hold-ms", "0")), Flag("modack"));
    return r.Collapsed ? 1 : 0;
  case "repro":
    return await Repro();
  default:
    Console.Error.WriteLine($"unknown command {command}");
    return 2;
}

// Fresh topic and subscription, then the sequence that collapsed 3 times out of 3 on 2026-11-18:
//   1. hold 200 ms, no modack    (drains normally)
//   2. hold 0, modack at receipt (drains normally)
//   3. hold 200 ms, no modack    (same as 1: collapses after ~400 000 messages)
async Task<int> Repro()
{
  var name = $"pubsub-collapse-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
  var topic = TopicName.FromProjectTopic(project, name);
  var subscription = SubscriptionName.FromProjectSubscription(project, name);
  var publisherApi = await new PublisherServiceApiClientBuilder { ChannelCredentials = channelCredentials }.BuildAsync();
  var subscriberApi = await new SubscriberServiceApiClientBuilder { ChannelCredentials = channelCredentials }.BuildAsync();
  await publisherApi.CreateTopicAsync(topic);
  await subscriberApi.CreateSubscriptionAsync(new Subscription
  {
    SubscriptionName = subscription,
    TopicAsTopicName = topic,
    AckDeadlineSeconds = 60,
    ExpirationPolicy = new ExpirationPolicy(), // never expires
  });
  Log($"created topic and subscription {name} (ack deadline 60 s), {consumers} consumers, {messages} messages per drain");

  var steps = new (string Name, int HoldMs, bool Modack)[]
  {
    ("1 hold 200 ms", 200, false),
    ("2 modack, ack at once", 0, true),
    ("3 hold 200 ms", 200, false),
  };
  var results = new List<(string, DrainResult)>();
  try
  {
    foreach (var (stepName, holdMs, modack) in steps)
    {
      Log($"== step {stepName}");
      // Drop what an earlier step left, as a fresh backlog for each drain
      await subscriberApi.SeekAsync(new SeekRequest { SubscriptionAsSubscriptionName = subscription, Time = Timestamp.FromDateTime(DateTime.UtcNow) });
      await Publish(topic, messages);
      results.Add((stepName, await Drain(subscription, messages, holdMs, modack)));
    }
  }
  finally
  {
    if (!Flag("keep"))
    {
      await subscriberApi.DeleteSubscriptionAsync(subscription);
      await publisherApi.DeleteTopicAsync(topic);
      Log($"deleted {name}");
    }
  }

  Console.WriteLine();
  Console.WriteLine("step                     delivered  peak msg/s  collapsed  at message  empty Pulls after");
  foreach (var (stepName, r) in results)
  {
    Console.WriteLine($"{stepName,-24} {r.Delivered,9}  {r.Peak,10}  {(r.Collapsed ? "YES" : "no"),9}  {(r.Collapsed ? r.CollapsedAt.ToString() : "-"),10}  {(r.Collapsed ? $"{r.EmptyShareAfter:P0}" : "-"),17}");
  }
  return results[^1].Item2.Collapsed ? 1 : 0;
}

async Task Publish(TopicName topic, int count)
{
  var publisher = await new PublisherClientBuilder
  {
    TopicName = topic,
    ClientCount = 4,
    ChannelCredentials = channelCredentials,
    Settings = new PublisherClient.Settings
    {
      BatchingSettings = new BatchingSettings(1000, 1_000_000, TimeSpan.FromMilliseconds(50)),
    },
  }.BuildAsync();
  var sw = Stopwatch.StartNew();
  var pending = new List<Task<string>>(20_000);
  for (var i = 0; i < count; i++)
  {
    pending.Add(publisher.PublishAsync(ByteString.CopyFromUtf8(Guid.NewGuid().ToString("N"))));
    if (pending.Count == 20_000)
    {
      await Task.WhenAll(pending);
      pending.Clear();
    }
  }
  await Task.WhenAll(pending);
  await publisher.ShutdownAsync(TimeSpan.FromSeconds(30));
  Log($"published {count} messages in {sw.Elapsed.TotalSeconds:F0} s");
}

async Task<DrainResult> Drain(SubscriptionName subscription, int expected, int holdMs, bool modack)
{
  // One client (one gRPC channel) per consumer, kept across drains like the connection of an agent
  if (consumerClients is null)
  {
    consumerClients = new SubscriberServiceApiClient[consumers];
    for (var i = 0; i < consumers; i++)
    {
      consumerClients[i] = await new SubscriberServiceApiClientBuilder { ChannelCredentials = channelCredentials }.BuildAsync();
    }
  }
  var clients = consumerClients;

  var seen = new ConcurrentDictionary<string, byte>();
  var durations = new ConcurrentQueue<double>();
  var errorKinds = new ConcurrentDictionary<string, int>();
  long pulls = 0, received = 0, empty = 0, errors = 0;
  var end = DateTime.UtcNow.AddSeconds(seconds);
  var cts = new CancellationTokenSource();

  async Task Consumer(SubscriberServiceApiClient client)
  {
    while (!cts.IsCancellationRequested && DateTime.UtcNow < end)
    {
      var sw = Stopwatch.StartNew();
      PullResponse response;
      try
      {
        response = await client.PullAsync(new PullRequest { SubscriptionAsSubscriptionName = subscription, MaxMessages = 1 },
                                          CallSettings.FromCancellationToken(cts.Token));
      }
      catch (Exception e) when (cts.IsCancellationRequested || e is RpcException { StatusCode: StatusCode.Cancelled })
      {
        return;
      }
      catch (Exception e)
      {
        Interlocked.Increment(ref errors);
        errorKinds.AddOrUpdate(e.GetType().Name + ": " + e.Message[..Math.Min(100, e.Message.Length)], 1, (_, n) => n + 1);
        await Task.Delay(1000);
        continue;
      }
      durations.Enqueue(sw.Elapsed.TotalMilliseconds);
      Interlocked.Increment(ref pulls);
      if (response.ReceivedMessages.Count == 0)
      {
        Interlocked.Increment(ref empty);
        continue;
      }
      foreach (var m in response.ReceivedMessages)
      {
        if (seen.TryAdd(m.Message.MessageId, 0))
        {
          Interlocked.Increment(ref received);
        }
      }
      var ackIds = response.ReceivedMessages.Select(m => m.AckId).ToArray();
      try
      {
        if (modack)
        {
          await client.ModifyAckDeadlineAsync(subscription, ackIds, 120);
        }
        if (holdMs > 0)
        {
          await Task.Delay(holdMs);
        }
        await client.AcknowledgeAsync(subscription, ackIds);
      }
      catch (Exception e)
      {
        Interlocked.Increment(ref errors);
        errorKinds.AddOrUpdate(e.GetType().Name + ": " + e.Message[..Math.Min(100, e.Message.Length)], 1, (_, n) => n + 1);
      }
    }
  }

  Log($"draining {subscription.SubscriptionId}: {consumers} consumers, hold {holdMs} ms, modack {(modack ? "yes" : "no")}, up to {seconds} s");
  var tasks = clients.Select(Consumer).ToArray();
  var result = new DrainResult();
  var start = Stopwatch.StartNew();
  long lastReceived = 0;
  while (!tasks.All(t => t.IsCompleted))
  {
    await Task.Delay(10_000);
    var d = new List<double>();
    while (durations.TryDequeue(out var x))
    {
      d.Add(x);
    }
    d.Sort();
    var p = Interlocked.Exchange(ref pulls, 0);
    var e = Interlocked.Exchange(ref empty, 0);
    var err = Interlocked.Exchange(ref errors, 0);
    var total = Interlocked.Read(ref received);
    var rate = (total - lastReceived) / 10;
    lastReceived = total;
    var emptyShare = p == 0 ? 0 : (double)e / p;
    result.Peak = Math.Max(result.Peak, rate);
    // Collapse: a quarter of the Pulls come back empty while more than 20 000 messages are still waiting
    var remaining = expected - total;
    if (!result.Collapsed && remaining > 20_000 && emptyShare >= 0.25)
    {
      result.Collapsed = true;
      result.CollapsedAt = total;
    }
    if (result.Collapsed && remaining > 0)
    {
      result.EmptyAfter += e;
      result.PullsAfter += p;
    }
    Log($"  +{start.Elapsed.TotalSeconds,4:F0} s  {rate,6} msg/s  empty {emptyShare,4:P0}  pull p50 {Q(d, .5),6} ms  p95 {Q(d, .95),6} ms  delivered {total,7}/{expected}" +
        (err > 0 ? $"  errors {err} {string.Join("; ", errorKinds.Select(k => $"{k.Value}x {k.Key}"))}" : ""));
    errorKinds.Clear();
    if (total >= expected)
    {
      cts.Cancel();
    }
  }
  result.Delivered = Interlocked.Read(ref received);
  Log($"  {(result.Collapsed ? $"COLLAPSED after {result.CollapsedAt} messages" : "drained normally")}: {result.Delivered}/{expected} in {start.Elapsed.TotalSeconds:F0} s, peak {result.Peak} msg/s");
  return result;
}

static string Q(List<double> v, double f) => v.Count == 0 ? "-" : v[Math.Min(v.Count - 1, (int)(f * v.Count))].ToString("F0");

static void Log(string line) => Console.WriteLine($"{DateTime.UtcNow:HH:mm:ss}Z {line}");

class DrainResult
{
  public long Delivered;
  public long Peak;
  public bool Collapsed;
  public long CollapsedAt;
  public long EmptyAfter;
  public long PullsAfter;
  public double EmptyShareAfter => PullsAfter == 0 ? 0 : (double)EmptyAfter / PullsAfter;
}
