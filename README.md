# Pub/Sub unary Pull collapse

A Pub/Sub subscription drained by many unary Pull consumers suddenly stops delivering: every consumer gets empty
Pulls of ~18 s while hundreds of thousands of messages are still waiting. Delivery falls from ~8000 to a few hundred
messages/s. No error, no expired ack deadline, nothing in Cloud Monitoring.

Found with [ArmoniK](https://github.com/aneoconsulting/ArmoniK): its agents are unary Pull consumers (one message
per Pull, ack after the task). This repository reproduces it without ArmoniK, with the same client library
(`Google.Cloud.PubSub.V1` 3.39.0).

## What the reproducer does

`dotnet run -- repro <project>` creates a fresh topic and subscription (ack deadline 60 s), then runs three drains of
500 000 messages on that same subscription, with 3000 consumers. Each consumer has its own client (own gRPC channel)
and loops on `Pull(maxMessages: 1)`, then acks:

| Step | Consumer loop | Result |
|---|---|---|
| 1 | Pull, wait 200 ms, ack | drains normally (~8300 msg/s) |
| 2 | Pull, `ModifyAckDeadline` 120 s, ack at once | drains normally (~8800 msg/s) |
| 3 | same as step 1 | **collapses** |

It deletes the topic and subscription at the end (`--keep` to keep them), prints a summary, and exits with 1 when the
last step collapsed. A drain counts as collapsed when a 10 s window has 25 % or more empty Pulls while over 20 000
messages are still waiting.

## Run it on GKE

It must run in the region of the topic. From a workstation a Pull takes 0.5-1.2 s instead of ~80 ms, so 3000
consumers stay around 2300 msg/s, below the rate that triggers the collapse (~6000 msg/s). One pod is enough; a GCE
VM with `dotnet run` would do too.

Requirements: a GKE cluster with Workload Identity, a node that fits 8 vCPU, and outbound internet access (NuGet).

1. Set your project in `kustomization.yaml` (`PROJECT=`).
2. Grant Pub/Sub to the Kubernetes service account of the Job:

   ```sh
   gcloud projects add-iam-policy-binding <project> --role=roles/pubsub.editor \
     --member=principal://iam.googleapis.com/projects/<project-number>/locations/global/workloadIdentityPools/<project>.svc.id.goog/subject/ns/pubsub-collapse/sa/pubsub-collapse
   ```

3. Run and follow (~15 min):

   ```sh
   kubectl apply -k .
   kubectl logs -n pubsub-collapse job/pubsub-collapse -f
   ```

To run it again: `kubectl delete job -n pubsub-collapse pubsub-collapse`, then step 3.

## Result (2026-11-18, europe-west9, one pod of 8 vCPU)

```
step                     delivered  peak msg/s  collapsed  at message  empty Pulls after
1 hold 200 ms               500000        8289         no           -                  -
2 modack, ack at once       500000        8802         no           -                  -
3 hold 200 ms               341801        8824        YES       67199               14 %
```

Step 3 delivered ~48 000 messages in 10 s, then 100-850 msg/s for ~5 min, with 20-30 % of the Pulls empty after
~18.4 s, then recovered:

```
+  10 s    4859 msg/s  empty  0 %  pull p50     77 ms  p95   1377 ms  delivered   48598/500000
+  20 s       5 msg/s  empty  0 %  pull p50   1143 ms  p95   1538 ms  delivered   48655/500000
+  50 s     783 msg/s  empty 25 %  pull p50     46 ms  p95  18386 ms  delivered   67199/500000
+ 100 s     119 msg/s  empty 10 %  pull p50     48 ms  p95  18368 ms  delivered   87989/500000
+ 300 s     678 msg/s  empty 25 %  pull p50     46 ms  p95  18388 ms  delivered  188292/500000
+ 360 s    8824 msg/s  empty  0 %  pull p50     41 ms  p95    906 ms  delivered  341801/500000
```

## Other observations

- Same sequence with 10 pods of 300 consumers: collapsed 3 times out of 3.
- With ArmoniK on a fresh subscription: ~5000-5600 msg/s held for 14 min, ~6000 msg/s collapsed after ~2 min.
- Under continuous publishing, a collapsed subscription stayed slow from 45 min to ~3 h. `seek` did not help;
  deleting and recreating it did. Other subscriptions of the project were not affected.
- Publishing and consuming at the same time on a collapsed subscription stays at ~170-200 msg/s from the start, with
  no fast phase first: with ArmoniK, ~170 tasks/s ended while clients submitted up to ~9000/s, the backlog reaching
  ~600 000-700 000 in 2.5 min. Same with two Core versions.
- StreamingPull (one `SubscriberClient` per consumer, flow control 1, 200 ms before the ack) drained a subscription
  just collapsed in unary Pull at ~3400 msg/s, while unary Pull collapsed again right after: the degraded state hits
  unary Pull only. StreamingPull above ~6000 msg/s sustained is not tested yet.
- Every drain starts with ~60 s at ~2000 msg/s (Pull ~1.3 s) before reaching full speed, from GKE and from a
  workstation alike.

## Other commands

```sh
dotnet run -c Release -- publish <project> <topic> <count>
dotnet run -c Release -- drain <project> <subscription> <expected> [--hold-ms 200] [--modack]
```

Options: `--consumers 3000`, `--messages 500000`, `--seconds 360` (per drain), `--keep`. Credentials: Application
Default Credentials.
