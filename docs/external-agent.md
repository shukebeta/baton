# External-agent role (`--agent-cmd`)

By default a served reply is a single Messages-API call. `--agent-cmd` instead
backs the role with a **full-tooled native agent CLI run headless** — one that
edits files and runs git/bash/MCP — driven entirely through the mailbox, with
**no tmux and no live TUI**. This is the tmux-free launch leaf for a non-tmux
team role: `baton serve --agent-cmd …` has no `TMAT_PANE` / `tmux` / pane-title
dependency anywhere.

Baton is a **pure backend-agnostic transport** here: it spawns `--agent-cmd`,
feeds the request body on stdin, reads the reply on stdout, and does nothing
else agent-specific. All agent knowledge — flag spelling, system prompt, MCP
config, permissions — lives in the caller's `--agent-cmd` program (a wrapper
script, as `mat` does) or is passed straight through via `--agent-arg`.

```
baton serve --inbox <dir> --outbox <dir> \
  --agent-cmd claude --agent-cwd /path/to/worktree \
  --agent-arg --append-system-prompt --agent-arg "$(cat /path/to/role-identity.txt)" \
  --agent-arg -p --agent-arg --dangerously-skip-permissions
```

- `--agent-cmd <program>` — the agent CLI to run once per message. On Windows,
  use the installed CLI command name (for example, `claude` or `codex`); Baton
  resolves its `.cmd` shim.
- `--agent-arg <arg>` — a fixed argument passed on every run (repeatable), e.g.
  headless/role flags. The request body is delivered on the agent's **stdin**;
  the agent's final **stdout** becomes the reply body (see the output adapter
  below).
- `--agent-cwd <dir>` — the working directory (a git worktree) for every run;
  defaults to the `serve` process's own cwd.
- `--agent-timeout-ms <n>` — read timeout for one agent run (default `600000`).
  Generous by design: an agent run is many tool calls, not one provider turn.

## Reply shape: the output adapter

By default the **whole** stdout is the reply body — correct for a backend that
prints only its final answer (e.g. `claude -p`). A *streaming* backend
(codex/copilot) interleaves tool/step chatter into stdout, which would leak into
the reply. `--agent-output` isolates the final result:

- `--agent-output raw` (default) — the whole stdout is the reply body.
- `--agent-output json` — the reply body is the string value at a result field
  in the agent's **final non-empty stdout line, parsed as a JSON object** — the
  `--output-format json`/`stream-json` convention. Chatter lines above that final
  line are dropped. Pair it with the backend's own structured-output flag via
  `--agent-arg` (e.g. `--agent-arg --output-format --agent-arg json`).
  - `--agent-result-key <key>` — the field to read (default `result`, matching
    `claude -p --output-format json`; set it to your backend's field, e.g.
    `message`). Valid only with `--agent-output json`.
  - If the final line is absent, is not a JSON object, lacks the key, or the
    key's value is not a string, the run becomes a synthesized delivered
    `kind: "error"` (never a stringified-JSON body).

## System prompt and MCP: the caller's job

Baton has no first-class flag for system prompt or MCP config in agent mode —
that would bake in one backend's flag spelling. Instead, pass them straight
through as `--agent-arg` values, exactly as the example above does for
`--append-system-prompt`:

```
--agent-arg --append-system-prompt --agent-arg "$(cat /path/to/role-identity.txt)"
--agent-arg --mcp-config --agent-arg /path/to/mcp.json
```

This composes with any backend: swap the flag spelling for whatever your
`--agent-cmd` program expects. `mat`'s own per-kind adapter builds its agent CLI
invocation this same way, driving baton purely as a transport.

In this mode `serve` loads **no `BatonConfig` and needs no API key** — the agent
carries its own credentials and MCP config, layered through the inherited
environment or the `--agent-arg` passthrough above. A role-less external-agent
serve also has no Baton role-home prerequisite; `--role` is only needed when
loading per-role identity or recording per-role sessions. Cross-message state is
the agent's own job: it reconstructs context across rounds from **durable
artifacts** (the git branch/worktree it shares run-to-run, the issue thread,
prior mailbox history), not an in-memory session — headless-per-message is the
model. An agent run that
exits 0 with a non-empty extracted result is wrapped into a `kind: "response"`
(it nests no `baton.exchange/v1` record, since a multi-tool run is not one
provider call baton can vouch for); a spawn failure, non-zero exit, empty output,
an unextractable JSON result, or a timeout becomes a synthesized delivered
`kind: "error"`.

`scripts/external-agent-proof.sh` is the runnable end-to-end proof (real agent +
real credentials, so **not** part of baton's no-API-key CI): it drives two
addressed rounds against a throwaway git worktree and asserts an observable side
effect (a commit) plus round-2 continuity on a durable artifact (a further
commit extending round 1's file). The hermetic machinery is covered by the
`ExternalAgentParticipant` unit tests.
