#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const skillRootDir = path.resolve(__dirname, "..");

const host = "127.0.0.1";
const port = Number(process.env.CHATGPT_PLUS_DEBUG_PORT || 9222);
const originUrl = "https://chatgpt.com/";
const timeoutMs = Number(process.env.CHATGPT_PLUS_TIMEOUT_MS || 1800000);
export const defaultTaskStoreDir = path.join(skillRootDir, ".state");
const taskStoreDir = process.env.CHATGPT_PLUS_TASK_DIR || defaultTaskStoreDir;
const doneMarker = "喵喵";
const searchPrefix = [
  "必须先使用 ChatGPT 的联网搜索能力，再回答下面的问题。",
  "不允许只依赖已有记忆直接作答。",
  "如果没有完成联网搜索，就不要开始正式回答。",
  "回答时必须优先基于搜索到的结果，并明确写出关键日期、来源类型、关键依据。",
  "如果证据不足或搜索不到，请明确写“证据不足”或“未搜索到可靠依据”，不要编造。",
  `回答结束后，最后一行必须单独输出“${doneMarker}”。`,
].join("\n");

function ensureTaskStore(dir = taskStoreDir) {
  fs.mkdirSync(dir, { recursive: true });
}

function taskPath(taskId, dir = taskStoreDir) {
  ensureTaskStore(dir);
  return path.join(dir, `${taskId}.json`);
}

export function buildSearchPrompt(prompt) {
  return `${searchPrefix}\n\n任务如下：\n${prompt}`;
}

export function createTaskRecord({ mode, prompt, sentPrompt, tabId, url, taskStoreDir: recordTaskStoreDir }) {
  const lines = String(prompt).split("\n").map((line) => line.trim()).filter(Boolean);
  const anchor = (lines.at(-1) || String(prompt)).trim();
  return {
    id: crypto.randomUUID(),
    mode,
    prompt,
    sentPrompt,
    anchor,
    tabId,
    url,
    taskStoreDir: recordTaskStoreDir || taskStoreDir,
    status: "submitted",
    submittedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    resultText: "",
  };
}

export function saveTaskRecord(task, dir = taskStoreDir) {
  fs.writeFileSync(taskPath(task.id, dir), JSON.stringify(task, null, 2));
}

export function loadTaskRecord(taskId, dir = taskStoreDir) {
  return JSON.parse(fs.readFileSync(taskPath(taskId, dir), "utf8"));
}

export function updateTaskRecord(taskId, patch, dir = taskStoreDir) {
  const current = loadTaskRecord(taskId, dir);
  const next = {
    ...current,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  saveTaskRecord(next, dir);
  return next;
}

export function buildWatcherSpawnSpec(taskId, options = {}) {
  const nodeBin = options.nodeBin || process.execPath;
  const scriptPath = path.resolve(options.scriptPath || __filename);
  const taskDir = options.taskDir || options.dir || taskStoreDir;
  return {
    command: nodeBin,
    args: [scriptPath, "watch", taskId],
    options: {
      detached: true,
      stdio: "ignore",
      env: {
        ...process.env,
        ...(options.env || {}),
        CHATGPT_PLUS_TASK_DIR: taskDir,
      },
    },
  };
}

export function startTaskWatcher(taskId, options = {}) {
  const dir = options.dir || taskStoreDir;
  const env = options.env || process.env;
  if (String(env.CHATGPT_PLUS_DISABLE_AUTO_WATCH || "") === "1") {
    return loadTaskRecord(taskId, dir);
  }

  const now = options.now || (() => new Date().toISOString());
  const spawnImpl = options.spawnImpl || spawn;
  try {
    const spec = buildWatcherSpawnSpec(taskId, {
      nodeBin: options.nodeBin,
      scriptPath: options.scriptPath,
      taskDir: dir,
      env,
    });
    const child = spawnImpl(spec.command, spec.args, spec.options);
    child.unref?.();
    return updateTaskRecord(
      taskId,
      {
        watcherPid: child.pid,
        watcherMode: "auto",
        watcherStartedAt: now(),
        watcherError: "",
      },
      dir,
    );
  } catch (error) {
    return updateTaskRecord(
      taskId,
      {
        watcherError: error?.message || String(error),
        watcherFinishedAt: now(),
      },
      dir,
    );
  }
}

async function httpJson(requestPath, options = {}) {
  const res = await fetch(`http://${host}:${port}${requestPath}`, options);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${requestPath}`);
  }
  return res.json();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class CDPClient {
  constructor(wsUrl) {
    this.ws = new WebSocket(wsUrl);
    this.nextId = 1;
    this.pending = new Map();
    this.ready = new Promise((resolve, reject) => {
      this.ws.addEventListener("open", resolve, { once: true });
      this.ws.addEventListener("error", reject, { once: true });
    });
    this.ws.addEventListener("message", (event) => {
      const msg = JSON.parse(event.data);
      if (!Object.prototype.hasOwnProperty.call(msg, "id")) return;
      const pending = this.pending.get(msg.id);
      if (!pending) return;
      this.pending.delete(msg.id);
      if (msg.error) pending.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else pending.resolve(msg.result);
    });
  }

  async send(method, params = {}) {
    await this.ready;
    const id = this.nextId++;
    const payload = { id, method, params };
    this.ws.send(JSON.stringify(payload));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  async close() {
    if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) {
      this.ws.close();
      await sleep(50);
    }
  }
}

async function listTabs() {
  return httpJson("/json/list");
}

async function newTab(url = originUrl) {
  return httpJson(`/json/new?${encodeURIComponent(url)}`, { method: "PUT" });
}

export async function closeTab(tabId, requester = httpJson) {
  if (!tabId) return null;
  return requester(`/json/close/${encodeURIComponent(tabId)}`, { method: "PUT" });
}

export function pickSubmissionTabFromList(_tabs = []) {
  return null;
}

export function pickTaskTabFromList(tabs, task = {}) {
  const pages = tabs.filter((tab) => tab.type === "page" && String(tab.url || "").startsWith("https://chatgpt.com"));
  if (task.tabId) {
    const exactId = pages.find((tab) => tab.id === task.tabId);
    if (exactId) return exactId;
  }
  if (task.url) {
    const exactUrl = pages.find((tab) => tab.url === task.url);
    if (exactUrl) return exactUrl;
  }
  return pages[0];
}

export function isRecoverableSubmissionError(error) {
  const message = error?.message || String(error || "");
  return /Promise was collected/i.test(message);
}

export function isRecoverableEvaluationError(error) {
  const message = error?.message || String(error || "");
  return /Cannot find default execution context|Execution context was destroyed/i.test(message);
}

export function didPromptAppearInProbe(probe, anchor) {
  const text = String(probe?.bodyText || "");
  return Boolean(anchor) && text.includes(anchor);
}

export function findPreferredReplyButtonIndex(buttons = []) {
  return buttons.findIndex((button) => /我更喜欢这个回复/.test(String(button?.text || "")));
}

export function classifyLoginProbe(probe = {}) {
  const url = String(probe.url || "");
  const loggedOut = Boolean(probe.loggedOut);
  const hasComposer = Boolean(probe.hasComposer);

  if (!url || url === "about:blank") {
    return { ok: false, reason: "blank_page" };
  }
  if (url.includes("/workspace/deactivated")) {
    return { ok: false, reason: "workspace_deactivated" };
  }
  if (loggedOut) {
    return { ok: false, reason: "logged_out" };
  }
  if (!hasComposer) {
    return { ok: false, reason: "missing_composer" };
  }
  return { ok: true, reason: "ok" };
}

async function ensureChatGPTTab() {
  const tabs = await listTabs();
  const reusable = pickSubmissionTabFromList(tabs);
  if (reusable) return reusable;
  return newTab(originUrl);
}

async function pickTaskTab(task) {
  const tabs = await listTabs();
  const picked = pickTaskTabFromList(tabs, task);
  if (picked) return picked;
  return ensureChatGPTTab();
}

async function evaluate(tab, expression) {
  const client = new CDPClient(tab.webSocketDebuggerUrl);
  try {
    await client.send("Page.enable");
    await client.send("Runtime.enable");
    const result = await client.send("Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    return result.result?.value;
  } finally {
    await client.close();
  }
}

async function evaluateWithRecovery(tab, expression, options = {}) {
  const retries = Number(options.retries ?? 10);
  const delayMs = Number(options.delayMs ?? 300);
  for (let i = 0; i < retries; i++) {
    try {
      return await evaluate(tab, expression);
    } catch (error) {
      if (!isRecoverableEvaluationError(error) || i === retries - 1) {
        throw error;
      }
      await sleep(delayMs);
    }
  }
}

async function probeSubmittedPrompt(tab, anchor) {
  return evaluateWithRecovery(
    tab,
    `(() => ({
      url: location.href,
      bodyText: document.body.innerText.slice(-30000)
    }))()`,
  );
}

async function resolvePreferredReplyChoice(task) {
  const tab = await pickTaskTab(task);
  return evaluateWithRecovery(
    tab,
    `(() => {
      const buttons = [...document.querySelectorAll('button')].map((button, index) => ({
        index,
        text: (button.innerText || '').trim(),
      }));
      const preferredIndex = ${findPreferredReplyButtonIndex.toString()}(buttons);
      if (preferredIndex < 0) {
        return { clicked: false };
      }
      const domButtons = [...document.querySelectorAll('button')];
      const button = domButtons[preferredIndex];
      button?.click();
      return { clicked: Boolean(button) };
    })()`,
  );
}

async function status() {
  const tab = await ensureChatGPTTab();
  const value = await evaluateWithRecovery(
    tab,
    `(() => ({
      title: document.title,
      url: location.href,
      readyState: document.readyState,
      loggedOut: /登录|Log in|Sign up|免费注册/.test(document.body.innerText),
      hasComposer: Boolean(document.querySelector('[contenteditable="true"]')),
      body: document.body.innerText.slice(0, 500)
    }))()`,
  );
  value.loginCheck = classifyLoginProbe(value);
  console.log(JSON.stringify(value, null, 2));
}

async function submitPrompt(prompt, mode = "plain") {
  const sentPrompt = mode === "search" ? buildSearchPrompt(prompt) : prompt;
  const tab = await ensureChatGPTTab();
  let loginState;
  let loginCheck = { ok: false, reason: "blank_page" };
  for (let i = 0; i < 10; i++) {
    try {
      loginState = await evaluateWithRecovery(
        tab,
        `(() => ({
          url: location.href,
          loggedOut: /登录|Log in|Sign up|免费注册/.test(document.body.innerText),
          hasComposer: Boolean(document.querySelector('[contenteditable="true"]'))
        }))()`,
      );
      loginCheck = classifyLoginProbe(loginState);
      if (loginCheck.ok) {
        break;
      }
      if (loginCheck.reason === "workspace_deactivated" || loginCheck.reason === "logged_out") {
        break;
      }
      await sleep(300);
    } catch (error) {
      if (!isRecoverableEvaluationError(error) || i === 9) {
        throw error;
      }
      await sleep(300);
    }
  }
  if (!loginCheck.ok) {
    const reasonMessages = {
      blank_page: "ChatGPT page did not finish loading and is still about:blank.",
      workspace_deactivated: "ChatGPT opened a workspace deactivated page instead of a usable chat session.",
      logged_out: "ChatGPT page is not logged in.",
      missing_composer: "ChatGPT page has no prompt composer, so the session is not usable.",
    };
    const baseMessage = reasonMessages[loginCheck.reason] || "ChatGPT page is not ready for prompt submission.";
    throw new Error(`${baseMessage} Ask the user to restore the automation Chrome login/session first.`);
  }

  let result;
  try {
    result = await evaluateWithRecovery(
      tab,
      `(async () => {
        const prompt = ${JSON.stringify(sentPrompt)};
        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

        const findComposer = () => document.querySelector('[contenteditable="true"]');

        const setComposerText = (box, text) => {
          box.focus();
          const p = box.querySelector('p');
          if (p) p.textContent = text;
          else box.textContent = text;
          box.dispatchEvent(new InputEvent('input', {
            bubbles: true,
            inputType: 'insertText',
            data: text
          }));
        };

        const findSendButton = (box) => {
          const root = box?.closest('form') || box?.parentElement || document;
          const buttons = [...root.querySelectorAll('button')];
          return buttons.find((button) => {
            const label = [
              button.getAttribute('aria-label'),
              button.getAttribute('title'),
              button.innerText,
              button.className,
            ].filter(Boolean).join(' ');
            return (
              !button.disabled &&
              (
                /发送|Send|submit|composer-submit-button-color/i.test(label) ||
                button.getAttribute('type') === 'submit'
              )
            );
          });
        };

        let box = null;
        for (let i = 0; i < 30; i++) {
          box = findComposer();
          if (box) break;
          await sleep(500);
        }
        if (!box) throw new Error('Prompt composer not found');

        setComposerText(box, prompt);
        await sleep(300);

        const sendButton = findSendButton(box);
        if (sendButton) {
          sendButton.click();
        } else {
          box.dispatchEvent(new KeyboardEvent('keydown', {
            key: 'Enter',
            code: 'Enter',
            which: 13,
            keyCode: 13,
            bubbles: true,
          }));
          box.dispatchEvent(new KeyboardEvent('keyup', {
            key: 'Enter',
            code: 'Enter',
            which: 13,
            keyCode: 13,
            bubbles: true,
          }));
        }

        return { sent: true, url: location.href };
      })()`,
    );
  } catch (error) {
    if (!isRecoverableSubmissionError(error)) {
      throw error;
    }
    let recovered = null;
    for (let i = 0; i < 5; i++) {
      await sleep(500);
      const probe = await probeSubmittedPrompt(tab, prompt);
      if (didPromptAppearInProbe(probe, prompt)) {
        recovered = { sent: true, url: probe.url || tab.url };
        break;
      }
    }
    if (!recovered) {
      throw error;
    }
    result = recovered;
  }

  if (!result?.sent) {
    throw new Error("Prompt was not sent");
  }

  const task = createTaskRecord({
    mode,
    prompt,
    sentPrompt,
    tabId: tab.id,
    url: result.url || tab.url,
    taskStoreDir,
  });
  saveTaskRecord(task);
  return task;
}

async function getTaskSnapshot(task) {
  const tab = await pickTaskTab(task);
  const snapshot = await evaluateWithRecovery(
    tab,
    `(() => {
      const text = document.body.innerText;
      const anchor = ${JSON.stringify(task.anchor || task.prompt)};
      const idx = text.lastIndexOf(anchor);
      const tail = idx >= 0 ? text.slice(idx, idx + 20000) : text.slice(-20000);
      const generating = /正在生成回复|Stop generating|停止生成/.test(text);
      const hasReply = /ChatGPT 说：[\\s\\S]*\\S/.test(tail);
      return { idx, tail, generating, hasReply, url: location.href };
    })()`,
  );
  return snapshot;
}

export function extractResultText(snapshot) {
  if (!snapshot?.tail) return "";
  const marker = "ChatGPT 说：";
  const start = snapshot.tail.indexOf(marker);
  if (start < 0) return "";

  const afterStart = snapshot.tail.slice(start);
  const nextUserTurn = afterStart.indexOf("\n你说：");
  if (nextUserTurn < 0) {
    return afterStart.trim();
  }
  return afterStart.slice(0, nextUserTurn).trim();
}

function sanitizeResultText(text) {
  const value = String(text || "");
  const markerPattern = new RegExp(`(^|\\n)${doneMarker}(?:\\n|$)`, "u");
  const markerMatch = value.match(markerPattern);
  const trimmedAtMarker = markerMatch
    ? value.slice(0, markerMatch.index + (markerMatch[1] ? markerMatch[1].length : 0))
    : value;
  return trimmedAtMarker
    .replace(/\n*思考\nChatGPT 也可能会犯错。请核查重要信息。查看 Cookie 首选项。\s*$/u, "")
    .trim();
}

function hasDoneMarker(text) {
  return new RegExp(`(^|\\n)${doneMarker}(?:\\n|$)`, "u").test(String(text || ""));
}

function isIntermediateResultText(text) {
  return /正在思考|正在搜索|Searching|ChatGPT 仍在生成回复|正在生成回复|Stop generating|停止生成/.test(String(text || ""));
}

function isIntermediateTail(text) {
  return /你更偏向于哪个回复|回复 1|回复 2|正在搜索|Searching|ChatGPT 仍在生成回复|正在思考|正在生成回复|Stop generating|停止生成/.test(String(text || ""));
}

export function classifyTaskSnapshot(snapshot, options = {}) {
  const requireDoneMarker = Boolean(options?.requireDoneMarker);
  const generating = Boolean(snapshot?.generating);
  const hasReply = Boolean(snapshot?.hasReply);
  if (snapshot?.idx < 0 || !hasReply) {
    return { status: "queued", resultText: "" };
  }
  const resultText = extractResultText(snapshot);
  const doneMarked = hasDoneMarker(resultText);
  if (doneMarked) {
    return { status: "done", resultText: sanitizeResultText(resultText) };
  }
  if (requireDoneMarker) {
    return { status: "generating", resultText: "" };
  }
  if (generating || isIntermediateResultText(resultText) || isIntermediateTail(snapshot?.tail)) {
    return { status: "generating", resultText: "" };
  }
  return { status: "done", resultText: sanitizeResultText(resultText) };
}

export async function finalizeCompletedTask(task, options = {}) {
  if (!task || task.status !== "done" || !task.tabId || task.closedAt) {
    return task;
  }
  const closeTabImpl = options.closeTabImpl || closeTab;
  const now = options.now || (() => new Date().toISOString());
  await closeTabImpl(task.tabId);
  return {
    ...task,
    closedAt: now(),
  };
}

async function getTaskStatus(taskId) {
  const task = loadTaskRecord(taskId);
  const snapshot = await getTaskSnapshot(task);
  const classified = classifyTaskSnapshot(snapshot, { requireDoneMarker: task.mode === "search" });
  let next = updateTaskRecord(taskId, {
    status: classified.status,
    url: snapshot.url || task.url,
    resultText: classified.resultText,
  });
  if (next.status === "done" && !next.closedAt) {
    try {
      const finalized = await finalizeCompletedTask(next);
      if (finalized?.closedAt) {
        next = updateTaskRecord(taskId, {
          closedAt: finalized.closedAt,
          closeError: "",
        });
      }
    } catch (error) {
      next = updateTaskRecord(taskId, {
        closeError: error?.message || String(error),
      });
    }
  }
  return next;
}

async function waitForTask(taskId, options = {}) {
  const emitResult = options.emitResult !== false;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const task = await getTaskStatus(taskId);
    if (task.status === "done") {
      if (emitResult) {
        console.log(task.resultText);
      }
      return;
    }
    if (task.status === "generating") {
      await resolvePreferredReplyChoice(task).catch(() => null);
    }
    await sleep(1000);
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for task ${taskId}`);
}

async function watchTask(taskId) {
  try {
    await waitForTask(taskId, { emitResult: false });
    updateTaskRecord(taskId, { watcherFinishedAt: new Date().toISOString(), watcherError: "" });
  } catch (error) {
    updateTaskRecord(taskId, {
      watcherFinishedAt: new Date().toISOString(),
      watcherError: error?.message || String(error),
    });
    throw error;
  }
}

async function main() {
  const command = process.argv[2];

  if (!command || ["help", "--help", "-h"].includes(command)) {
    console.log(`Usage:
  chatgpt_cdp.mjs open
  chatgpt_cdp.mjs status
  chatgpt_cdp.mjs send "your prompt"
  chatgpt_cdp.mjs send-search "your prompt"
  chatgpt_cdp.mjs submit "your prompt"
  chatgpt_cdp.mjs submit-search "your prompt"
  chatgpt_cdp.mjs watch "<task-id>"
  chatgpt_cdp.mjs task-status "<task-id>"
  chatgpt_cdp.mjs result "<task-id>"
  chatgpt_cdp.mjs wait "<task-id>"
`);
    return;
  }

  await httpJson("/json/version").catch(() => {
    throw new Error(`Chrome debugging endpoint not available on http://${host}:${port}. Start the automation browser first.`);
  });

  if (command === "open") {
    const tab = await ensureChatGPTTab();
    console.log(JSON.stringify({ url: tab.url, id: tab.id }, null, 2));
    return;
  }

  if (command === "status") {
    await status();
    return;
  }

  if (command === "send") {
    const prompt = process.argv.slice(3).join(" ").trim();
    if (!prompt) {
      throw new Error("Missing prompt text");
    }
    const task = await submitPrompt(prompt, "plain");
    await waitForTask(task.id);
    return;
  }

  if (command === "send-search") {
    const prompt = process.argv.slice(3).join(" ").trim();
    if (!prompt) {
      throw new Error("Missing prompt text");
    }
    const task = await submitPrompt(prompt, "search");
    await waitForTask(task.id);
    return;
  }

  if (command === "submit") {
    const prompt = process.argv.slice(3).join(" ").trim();
    if (!prompt) {
      throw new Error("Missing prompt text");
    }
    const task = await submitPrompt(prompt, "plain");
    const watched = startTaskWatcher(task.id);
    console.log(JSON.stringify(watched, null, 2));
    return;
  }

  if (command === "submit-search") {
    const prompt = process.argv.slice(3).join(" ").trim();
    if (!prompt) {
      throw new Error("Missing prompt text");
    }
    const task = await submitPrompt(prompt, "search");
    const watched = startTaskWatcher(task.id);
    console.log(JSON.stringify(watched, null, 2));
    return;
  }

  if (command === "watch") {
    const taskId = process.argv[3];
    if (!taskId) {
      throw new Error("Missing task id");
    }
    await watchTask(taskId);
    return;
  }

  if (command === "task-status") {
    const taskId = process.argv[3];
    if (!taskId) {
      throw new Error("Missing task id");
    }
    const task = await getTaskStatus(taskId);
    console.log(JSON.stringify(task, null, 2));
    return;
  }

  if (command === "result") {
    const taskId = process.argv[3];
    if (!taskId) {
      throw new Error("Missing task id");
    }
    const task = await getTaskStatus(taskId);
    console.log(task.resultText || "");
    return;
  }

  if (command === "wait") {
    const taskId = process.argv[3];
    if (!taskId) {
      throw new Error("Missing task id");
    }
    await waitForTask(taskId);
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

function isEntrypoint() {
  return process.argv[1] && path.resolve(process.argv[1]) === __filename;
}

if (isEntrypoint()) {
  main().catch((error) => {
    console.error(error.message || String(error));
    process.exit(1);
  });
}
