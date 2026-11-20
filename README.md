# Flowrensic Go SDK

Flowrensic runs beside your durable workflow engine and finds problems in your workflows. This is the Go SDK of Temporal Flowrensic, the edition for Temporal.

## What it does today

- Connects a Temporal Go worker to your Flowrensic server, so the worker shows up in Flowrensic. Call client.Connect from go.flowrensic.com/temporal-flowrensic-go-sdk/client with ServerBaseURL, CurrentEnv, EnvAccessKey and CurrentEphemeralRun. Every call sends the env, its access key, the ephemeral run and the worker id in the headers Flowrensic-Current-Env, Flowrensic-Env-Access-Key, Flowrensic-Current-Ephemeral-Run and Flowrensic-Worker-Id. The SDK reads no environment variable.
- The worker starts even when Flowrensic does not answer: by default it keeps trying in the background. NoAnswer picks another way: client.Fail, client.Retry, client.GiveUpQuietly or client.ForwardAnyway.
- Worker health: on the interval Flowrensic answers with, the worker says it is up, with its Version and the names of its WorkflowInterceptors. For a CI run it forwards the engine's metrics from MetricsAddress when Flowrensic asks, its own metrics from WorkerMetricsAddress, and the histories of the runs it saw, read with your TemporalClient. Call Shutdown on the client when the worker stops, so the run is handed over. Give client.Sink() to the activity interceptor.
- Records each activity attempt: its result, error and time. Make a sink with client.NewAttemptSink, and register activity.NewInterceptor(sink, activity.Options{Version: ...}) from go.flowrensic.com/temporal-flowrensic-go-sdk/activity in worker.Options.Interceptors. The sink sends the attempts in batches and never makes an activity wait on Flowrensic. It also sends what it dropped and the time the SDK takes on your worker. Call Close on the sink when the worker stops.
- Records the queries and signals of your app's Temporal client that fail: put client.NewClientInterceptor in client.Options.Interceptors, with the Namespace of that client.
- Counts the queries your workflows answer: put workflow.NewQueryInterceptor from go.flowrensic.com/temporal-flowrensic-go-sdk/workflow in worker.Options.Interceptors. It only reads, so a workflow runs and replays the same.
- Replay before deploy, for a CI job: client.FetchReplaySet downloads the histories Flowrensic picked into a folder, you replay each with Temporal's worker.WorkflowReplayer against the new code, and client.PublishVerdicts sends the results. The current env must be of kind ephemeral; a persistent env is refused.
- Deploy reports from CI: client.ReportDeployStage sends one stage of a deploy, with a time limit.

## Planned

- Each part the other Flowrensic SDKs get comes to this SDK too, so a Go worker sends the same data as one on the Node SDK, @flowrensic/temporal-flowrensic-node-sdk.

## Install

```sh
go get go.flowrensic.com/temporal-flowrensic-go-sdk
```
