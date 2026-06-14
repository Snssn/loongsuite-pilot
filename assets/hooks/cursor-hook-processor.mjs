#!/usr/bin/env node
/**
 * Cursor hook processor for loongsuite-pilot.
 *
 * Stateful processor: each hook event is appended to an event journal.
 * On parent "stop", all journal events are assembled into canonical history
 * records with proper step division, subagent nesting, and trace ids.
 *
 * History JSONL is the sole formal data source for CursorHookInput.
 * Raw capture is behind LOONGSUITE_CURSOR_RAW_TRACE=1 env flag.
 */

import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  hashJson,
  loadHookRuntimeConfig,
  sanitizeObject,
} from './agent-event-normalizer.mjs';
import { toInternalEvent } from './cursor/source-event.mjs';
import { appendEvent, readAllEvents, rewriteJournal } from './cursor/event-journal.mjs';
import { assembleTurn } from './cursor/react-assembler.mjs';

function resolveDataDir() {
  const configured = process.env.LOONGSUITE_PILOT_DATA_DIR;
  if (configured) return configured;
  return path.join(os.homedir(), '.loongsuite-pilot');
}

function localDateString(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

async function appendErrorJsonl(dataDir, now, fields) {
  const day = localDateString(now);
  const record = sanitizeObject({
    time: now.toISOString(),
    clientType: 'CursorHook',
    ...fields,
  }) || { time: now.toISOString(), clientType: 'CursorHook', stage: 'unknown' };
  const candidates = [
    path.join(dataDir, 'logs', 'cursor', 'errors', `cursor-error-${day}.jsonl`),
    path.join(os.tmpdir(), 'loongsuite-pilot', 'cursor', 'errors', `cursor-error-${day}.jsonl`),
  ];
  for (const filePath of candidates) {
    try {
      await fs.mkdir(path.dirname(filePath), { recursive: true });
      await fs.appendFile(filePath, `${JSON.stringify(record)}\n`, 'utf-8');
      return;
    } catch {
      // best-effort
    }
  }
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString('utf-8');
}

async function appendJsonl(filePath, record) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.appendFile(filePath, `${JSON.stringify(record)}\n`, 'utf-8');
}

async function appendBatchJsonl(filePath, records) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const content = records.map(r => JSON.stringify(r)).join('\n') + '\n';
  await fs.appendFile(filePath, content, 'utf-8');
}

function writeEmptyResponse() {
  process.stdout.write('{}\n');
}

const CLI_VERSION_PATTERN = /^\d{4}\.\d{2}\.\d{2}/;

function inferVariant(events) {
  for (const ev of events) {
    if (ev.cursor_version && CLI_VERSION_PATTERN.test(ev.cursor_version)) return 'cursor-cli';
  }
  return 'cursor';
}

async function main() {
  const dataDir = resolveDataDir();
  const raw = await readStdin();
  if (!raw || raw.trim().length === 0) {
    writeEmptyResponse();
    return;
  }

  const now = new Date();
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (err) {
    await appendErrorJsonl(dataDir, now, {
      stage: 'parse',
      'error.type': 'invalid_json',
      'error.message': err instanceof Error ? err.message : String(err),
      input_bytes: Buffer.byteLength(raw),
      input_sha256: hashJson(raw),
    });
    writeEmptyResponse();
    return;
  }

  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    await appendErrorJsonl(dataDir, now, {
      stage: 'validate',
      'error.type': 'invalid_payload_root',
      'error.message': 'Expected JSON object root payload',
      input_bytes: Buffer.byteLength(raw),
      input_sha256: hashJson(raw),
    });
    writeEmptyResponse();
    return;
  }

  // Convert to internal event and append to journal
  const internalEvent = toInternalEvent(payload);
  try {
    appendEvent(internalEvent);
  } catch (err) {
    await appendErrorJsonl(dataDir, now, {
      stage: 'journal_append',
      'error.type': 'journal_failed',
      'error.message': err instanceof Error ? err.message : String(err),
      hookEvent: internalEvent.hook_event,
    });
    writeEmptyResponse();
    return;
  }

  if (process.env.LOONGSUITE_CURSOR_RAW_TRACE === '1') {
    try {
      const rawFile = path.join(dataDir, 'logs', 'cursor', 'raw', 'cursor-raw-trace.jsonl');
      await appendJsonl(rawFile, { _captured_at: now.toISOString(), ...payload });
    } catch {
      // best-effort
    }
  }

  // ─── Deferred-stop logic ───
  // Cursor CLI fires stop BEFORE afterAgentResponse. If there's a prompt but
  // no response yet for this conversation, defer assembly until the late
  // response arrives.
  const shouldAssemble = await (async () => {
    if (internalEvent.hook_event === 'stop') {
      const allEvents = readAllEvents();
      const convId = internalEvent.conversation_id;
      const hasPrompt = allEvents.some(e => e.hook_event === 'beforeSubmitPrompt' && e.conversation_id === convId);
      const hasResponse = allEvents.some(e => e.hook_event === 'afterAgentResponse' && e.conversation_id === convId);
      if (hasPrompt && !hasResponse) return null; // defer
      return { allEvents, convId, transcriptPath: internalEvent.transcript_path };
    }
    if (internalEvent.hook_event === 'afterAgentResponse') {
      const allEvents = readAllEvents();
      const convId = internalEvent.conversation_id;
      const hasStop = allEvents.some(e => e.hook_event === 'stop' && e.conversation_id === convId);
      if (hasStop) {
        const stopEv = allEvents.find(e => e.hook_event === 'stop' && e.conversation_id === convId);
        return { allEvents, convId, transcriptPath: stopEv?.transcript_path };
      }
      return null;
    }
    return null;
  })();

  if (shouldAssemble) {
    try {
      const { allEvents, convId, transcriptPath } = shouldAssemble;
      const runtimeConfig = loadHookRuntimeConfig(dataDir);
      const variant = inferVariant(allEvents);
      const { records, consumedConversationIds } = assembleTurn(allEvents, {
        runtimeConfig,
        variant,
        stopConversationId: convId,
        transcriptPath,
      });

      if (records.length > 0) {
        const day = localDateString(now);
        const historyFile = path.join(dataDir, 'logs', 'cursor', 'history', `cursor-${day}.jsonl`);
        await appendBatchJsonl(historyFile, records);
      }

      const pendingTurnConvIds = new Set();
      const remaining = [];
      for (const ev of allEvents) {
        if (consumedConversationIds.has(ev.conversation_id)) continue;
        if (ev.hook_event === 'beforeSubmitPrompt') pendingTurnConvIds.add(ev.conversation_id);
      }
      for (const ev of allEvents) {
        if (consumedConversationIds.has(ev.conversation_id)) continue;
        if (pendingTurnConvIds.has(ev.conversation_id)) remaining.push(ev);
      }
      rewriteJournal(remaining, allEvents);
    } catch (err) {
      await appendErrorJsonl(dataDir, now, {
        stage: 'assemble',
        'error.type': 'assemble_failed',
        'error.message': err instanceof Error ? err.message : String(err),
      });
    }
  }

  writeEmptyResponse();
}

main().catch(async err => {
  await appendErrorJsonl(resolveDataDir(), new Date(), {
    stage: 'runtime',
    'error.type': 'unhandled_exception',
    'error.message': err instanceof Error ? err.message : String(err),
  });
  writeEmptyResponse();
});
