// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

// Package batch provides resumable asynchronous batch orchestration with
// idempotent task keys, durable step checkpoints, expiration, leases and
// rate limiting.
//
// # Model
//
// A task is an ordered list of named steps registered under a task type.
// Callers submit a task with an idempotency key; the processor persists a
// record for that key and executes the steps. After every successful step
// the next-step cursor is checkpointed to the store. While a worker is
// executing it holds an exclusive, heartbeat-renewed lease on the record.
//
//   - Idempotent keys: [Processor.Submit] creates the record atomically only
//     if the key is unknown. Repeated submissions (concurrent duplicates,
//     retries after completion, or redeliveries after a restart) observe the
//     single original outcome and never execute steps twice.
//   - Resumable restarts: a stopped process leaves records in pending or
//     running state. A replacement processor sharing the same [Store] and
//     [Registry] discovers them on startup and on a poll, claims running
//     records once their lease has lapsed, and resumes at the checkpointed
//     next step.
//   - Duplicate delivery: execution ownership is taken exclusively in
//     [Store.Claim] and progress is written with optimistic version checks.
//     Two processors that receive the same delivery cannot both execute a
//     step; a worker whose lease is fenced away fails its later commits and
//     stops touching the record.
//   - Expiration: tasks carry either a per-submission TTL ([WithTTL]) or the
//     processor default. Past-deadline tasks transition to expired at claim
//     time, before a step starts, or during periodic sweeps, without
//     producing side effects.
//   - Rate limiting and cancellation: a shared [Limiter] gates every step
//     start across all workers; step contexts are canceled on shutdown.
//
// # Delivery guarantee
//
// Checkpoints delineate the guarantee: the effects of a fully checkpointed
// step are never repeated, including across process restarts and
// redelivered messages. If a process stops in the narrow window after a
// step's effect but before its checkpoint commits, the step may run again
// after lease recovery (at-least-once at the step boundary). Steps that
// perform externally non-idempotent effects should either be inherently
// idempotent or arrange for their effect and the checkpoint commit to be the
// durable commit point.
//
// # Minimal example
//
//	registry := batch.NewRegistry()
//	registry.Register("example",
//	    batch.Step{Name: "prepare", Run: prepare},
//	    batch.Step{Name: "apply", Run: apply},
//	)
//	p, err := batch.NewProcessor(batch.Config{
//	    Store:         batch.NewMemoryStore(),
//	    Registry:      registry,
//	    Workers:       4,
//	    Limiter:       batch.NewTokenBucket(50, 1),
//	    LeaseTTL:      10 * time.Second,
//	    PollInterval:  time.Second,
//	})
//	if err != nil { /* ... */ }
//	if err := p.Start(ctx); err != nil { /* ... */ }
//	h, err := p.Submit(ctx, batch.Task{Key: "request-7", Type: "example"})
//	result, err := h.Wait(ctx)
package batch
