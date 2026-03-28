import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

import {
  buildWatcherSpawnSpec,
  buildSearchPrompt,
  classifyLoginProbe,
  classifyTaskSnapshot,
  closeTab,
  createTaskRecord,
  defaultTaskStoreDir,
  didPromptAppearInProbe,
  extractResultText,
  finalizeCompletedTask,
  findPreferredReplyButtonIndex,
  isRecoverableEvaluationError,
  isRecoverableSubmissionError,
  loadTaskRecord,
  pickTaskTabFromList,
  pickSubmissionTabFromList,
  saveTaskRecord,
  startTaskWatcher,
  updateTaskRecord,
} from "../scripts/chatgpt_cdp.mjs";

test("classifyLoginProbe treats about:blank as invalid session even if loggedOut is false", () => {
  const probe = {
    url: "about:blank",
    loggedOut: false,
    hasComposer: false,
  };

  const classified = classifyLoginProbe(probe);
  assert.equal(classified.ok, false);
  assert.equal(classified.reason, "blank_page");
});

test("classifyLoginProbe treats workspace deactivated page as invalid session", () => {
  const probe = {
    url: "https://chatgpt.com/workspace/deactivated",
    loggedOut: false,
    hasComposer: false,
  };

  const classified = classifyLoginProbe(probe);
  assert.equal(classified.ok, false);
  assert.equal(classified.reason, "workspace_deactivated");
});

test("classifyLoginProbe requires composer on chatgpt page before submission", () => {
  const probe = {
    url: "https://chatgpt.com/c/abc123",
    loggedOut: false,
    hasComposer: false,
  };

  const classified = classifyLoginProbe(probe);
  assert.equal(classified.ok, false);
  assert.equal(classified.reason, "missing_composer");
});

test("classifyLoginProbe accepts normal logged-in chatgpt page with composer", () => {
  const probe = {
    url: "https://chatgpt.com/c/abc123",
    loggedOut: false,
    hasComposer: true,
  };

  const classified = classifyLoginProbe(probe);
  assert.equal(classified.ok, true);
  assert.equal(classified.reason, "ok");
});

test("defaultTaskStoreDir points to project-local state directory", () => {
  assert.match(defaultTaskStoreDir, /case_data\/skills\/chatgpt-plus-browser\/\.state$/);
  assert.doesNotMatch(defaultTaskStoreDir, /\.codex\/skills\/chatgpt-plus-browser/);
});

test("buildSearchPrompt prepends hard search requirement", () => {
  const prompt = buildSearchPrompt("Only reply OK");
  assert.match(prompt, /必须先使用 ChatGPT 的联网搜索能力/);
  assert.match(prompt, /Only reply OK/);
  assert.match(prompt, /最后一行必须单独输出“喵喵”/);
});

test("createTaskRecord builds pending task metadata", () => {
  const task = createTaskRecord({
    mode: "search",
    prompt: "abc",
    sentPrompt: "wrapped abc",
    tabId: "tab-1",
    url: "https://chatgpt.com/c/test",
    taskStoreDir: "/tmp/task-store",
  });
  assert.equal(task.mode, "search");
  assert.equal(task.prompt, "abc");
  assert.equal(task.sentPrompt, "wrapped abc");
  assert.equal(task.tabId, "tab-1");
  assert.equal(task.url, "https://chatgpt.com/c/test");
  assert.equal(task.taskStoreDir, "/tmp/task-store");
  assert.equal(task.status, "submitted");
  assert.ok(task.id);
  assert.ok(task.submittedAt);
});

test("save/load/update task record persists JSON state", () => {
  const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "chatgpt-cdp-"));
  const task = createTaskRecord({
    mode: "plain",
    prompt: "hello",
    sentPrompt: "hello",
    tabId: "tab-2",
    url: "https://chatgpt.com/c/demo",
    taskStoreDir: tmpdir,
  });

  saveTaskRecord(task, tmpdir);
  const loaded = loadTaskRecord(task.id, tmpdir);
  assert.equal(loaded.id, task.id);
  assert.equal(loaded.status, "submitted");

  updateTaskRecord(task.id, { status: "done", resultText: "OK" }, tmpdir);
  const updated = loadTaskRecord(task.id, tmpdir);
  assert.equal(updated.status, "done");
  assert.equal(updated.resultText, "OK");
});

test("classifyTaskSnapshot keeps task in generating when reply is partial", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n\n我先按你\n\n思考\nChatGPT 也可能会犯错。请核查重要信息。查看 Cookie 首选项。\nChatGPT 仍在生成回复…",
    generating: true,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "generating");
  assert.equal(classified.resultText, "");
});

test("classifyTaskSnapshot returns done only for completed final reply", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n\n主因：2025-01-01｜来源｜内容｜解释\n备选：2025-01-02｜来源｜内容｜解释\n搜索依据：关键证据\n\n思考\nChatGPT 也可能会犯错。请核查重要信息。查看 Cookie 首选项。",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "done");
  assert.match(classified.resultText, /主因：/);
  assert.doesNotMatch(classified.resultText, /ChatGPT 也可能会犯错/);
});

test("classifyTaskSnapshot keeps task in generating when result text is still thinking", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n正在思考\n\n思考\nChatGPT 也可能会犯错。请核查重要信息。查看 Cookie 首选项。\nChatGPT 仍在生成回复…",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "generating");
  assert.equal(classified.resultText, "");
});

test("classifyTaskSnapshot keeps task in generating during A/B reply selection state", () => {
  const snapshot = {
    idx: 10,
    tail: "你更偏向于哪个回复？\n回复 1\n先做一次联网核验\n正在思考\n回复 2\n先做一次最小联网核验\n正在思考\n思考\nChatGPT 也可能会犯错。",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "generating");
  assert.equal(classified.resultText, "");
});

test("classifyTaskSnapshot keeps task in generating while search is in progress", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n先做一次联网核验\n正在搜索 www.calculatorsoup.com\n思考\nChatGPT 也可能会犯错。",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "generating");
  assert.equal(classified.resultText, "");
});

test("classifyTaskSnapshot returns done when reply ends with sentinel even if tail contains search phrases", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n主因：机器人产业链扩散\n备选：节后回流\n搜索依据：公司纪要与板块催化\n正在搜索 其他补充材料\n喵喵",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot);
  assert.equal(classified.status, "done");
  assert.match(classified.resultText, /主因：机器人产业链扩散/);
  assert.doesNotMatch(classified.resultText, /喵喵\s*$/);
});

test("classifyTaskSnapshot strips footer text that appears after sentinel", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n官网域名：openai.com\n\n喵喵\n\n来源\n\n发散性",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot, { requireDoneMarker: true });
  assert.equal(classified.status, "done");
  assert.equal(classified.resultText, "ChatGPT 说：\n官网域名：openai.com");
});

test("classifyTaskSnapshot keeps search task generating until sentinel appears", () => {
  const snapshot = {
    idx: 10,
    tail: "ChatGPT 说：\n官网域名：openai.com\n\n来源\n\n发散性",
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot, { requireDoneMarker: true });
  assert.equal(classified.status, "generating");
  assert.equal(classified.resultText, "");
});

test("extractResultText keeps the anchored reply even if later prompts exist in same chat", () => {
  const snapshot = {
    idx: 10,
    tail: [
      "任务如下：永鼎股份 W1/W2",
      "",
      "ChatGPT 说：",
      "W1主因：核聚变",
      "W2主因：光通信",
      "喵喵",
      "",
      "你说：",
      "只回答OK，最后一行输出喵喵。",
      "",
      "ChatGPT 说：",
      "OK",
      "喵喵",
      "",
      "你说：",
      "联网搜索后，只回答一行：OK。最后一行输出喵喵。",
      "",
      "ChatGPT 说：",
      "正在思考",
    ].join("\n"),
  };

  const resultText = extractResultText(snapshot);
  assert.match(resultText, /W1主因：核聚变/);
  assert.match(resultText, /W2主因：光通信/);
  assert.doesNotMatch(resultText, /^ChatGPT 说：\n正在思考/m);
});

test("classifyTaskSnapshot returns done for the anchored reply even if later prompts exist in same chat", () => {
  const snapshot = {
    idx: 10,
    tail: [
      "任务如下：永鼎股份 W1/W2",
      "",
      "ChatGPT 说：",
      "W1主因：核聚变",
      "W2主因：光通信",
      "喵喵",
      "",
      "你说：",
      "只回答OK，最后一行输出喵喵。",
      "",
      "ChatGPT 说：",
      "OK",
      "喵喵",
      "",
      "你说：",
      "联网搜索后，只回答一行：OK。最后一行输出喵喵。",
      "",
      "ChatGPT 说：",
      "正在思考",
    ].join("\n"),
    generating: false,
    hasReply: true,
  };

  const classified = classifyTaskSnapshot(snapshot, { requireDoneMarker: true });
  assert.equal(classified.status, "done");
  assert.match(classified.resultText, /W1主因：核聚变/);
  assert.match(classified.resultText, /W2主因：光通信/);
  assert.doesNotMatch(classified.resultText, /^ChatGPT 说：\n正在思考/m);
});

test("pickTaskTabFromList prefers exact tab id over other chatgpt tabs", () => {
  const tabs = [
    { id: "tab-a", url: "https://chatgpt.com/c/old", type: "page" },
    { id: "tab-b", url: "https://chatgpt.com/c/new", type: "page" },
  ];
  const task = { tabId: "tab-b", url: "https://chatgpt.com/c/new" };

  const picked = pickTaskTabFromList(tabs, task);
  assert.equal(picked.id, "tab-b");
});

test("pickTaskTabFromList falls back to exact url when tab id changed", () => {
  const tabs = [
    { id: "tab-a", url: "https://chatgpt.com/c/old", type: "page" },
    { id: "tab-b", url: "https://chatgpt.com/c/new", type: "page" },
  ];
  const task = { tabId: "missing", url: "https://chatgpt.com/c/new" };

  const picked = pickTaskTabFromList(tabs, task);
  assert.equal(picked.id, "tab-b");
});

test("pickSubmissionTabFromList never reuses existing chatgpt conversation pages", () => {
  const tabs = [
    { id: "tab-a", url: "https://chatgpt.com/", type: "page" },
    { id: "tab-b", url: "https://chatgpt.com/c/existing", type: "page" },
  ];

  const picked = pickSubmissionTabFromList(tabs);
  assert.equal(picked, null);
});

test("isRecoverableSubmissionError matches collected promise failures", () => {
  assert.equal(isRecoverableSubmissionError(new Error("Promise was collected")), true);
  assert.equal(isRecoverableSubmissionError(new Error("other failure")), false);
});

test("isRecoverableEvaluationError matches missing execution context failures", () => {
  assert.equal(isRecoverableEvaluationError(new Error("Cannot find default execution context")), true);
  assert.equal(isRecoverableEvaluationError(new Error("Execution context was destroyed")), true);
  assert.equal(isRecoverableEvaluationError(new Error("other failure")), false);
});

test("didPromptAppearInProbe detects prompt anchor in page text", () => {
  const probe = {
    bodyText: "你说：\n必须联网搜索后再回答。skill final verify 2026-03-12\n\nChatGPT 说：先做一次检索",
  };
  assert.equal(didPromptAppearInProbe(probe, "必须联网搜索后再回答。skill final verify 2026-03-12"), true);
  assert.equal(didPromptAppearInProbe(probe, "不存在的anchor"), false);
});

test("findPreferredReplyButtonIndex finds A/B preference button", () => {
  const buttons = [
    { text: "复制" },
    { text: "我更喜欢这个回复" },
    { text: "重新生成" },
  ];
  assert.equal(findPreferredReplyButtonIndex(buttons), 1);
  assert.equal(findPreferredReplyButtonIndex([{ text: "复制" }]), -1);
});

test("importing module does not execute CLI main", () => {
  const scriptPath = path.resolve(
    "/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data/skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs",
  );
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", `await import(${JSON.stringify(scriptPath)});`], {
    encoding: "utf8",
  });

  assert.equal(result.status, 0);
  assert.equal(result.stdout.trim(), "");
  assert.equal(result.stderr.trim(), "");
});

test("start_chrome script uses non-codex default profile path", () => {
  const scriptPath = path.resolve(
    "/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data/skills/chatgpt-plus-browser/scripts/start_chrome.sh",
  );
  const content = fs.readFileSync(scriptPath, "utf8");

  assert.match(content, /CHATGPT_PLUS_PROFILE_DIR/);
  assert.doesNotMatch(content, /\.codex\/browser-profiles/);
});

test("closeTab requests Chrome close endpoint for the finished task tab", async () => {
  const calls = [];
  const requester = async (requestPath, options = {}) => {
    calls.push({ requestPath, options });
    return { ok: true };
  };

  await closeTab("tab-123", requester);

  assert.deepEqual(calls, [
    {
      requestPath: "/json/close/tab-123",
      options: { method: "PUT" },
    },
  ]);
});

test("finalizeCompletedTask closes the finished task tab once and marks closedAt", async () => {
  const closeCalls = [];
  const task = {
    id: "task-1",
    status: "done",
    tabId: "tab-123",
    resultText: "OK",
  };

  const updated = await finalizeCompletedTask(task, {
    closeTabImpl: async (tabId) => {
      closeCalls.push(tabId);
    },
    now: () => "2026-03-22T10:00:00.000Z",
  });

  assert.deepEqual(closeCalls, ["tab-123"]);
  assert.equal(updated.closedAt, "2026-03-22T10:00:00.000Z");
});

test("buildWatcherSpawnSpec builds detached watch command with task dir env", () => {
  const spec = buildWatcherSpawnSpec("task-123", {
    nodeBin: "node-custom",
    scriptPath: "/tmp/chatgpt_cdp.mjs",
    taskDir: "/tmp/task-store",
    env: { EXTRA_FLAG: "1" },
  });

  assert.equal(spec.command, "node-custom");
  assert.deepEqual(spec.args, ["/tmp/chatgpt_cdp.mjs", "watch", "task-123"]);
  assert.equal(spec.options.detached, true);
  assert.equal(spec.options.stdio, "ignore");
  assert.equal(spec.options.env.CHATGPT_PLUS_TASK_DIR, "/tmp/task-store");
  assert.equal(spec.options.env.EXTRA_FLAG, "1");
});

test("createTaskRecord defaults taskStoreDir to project-local state path", () => {
  const task = createTaskRecord({
    mode: "plain",
    prompt: "hello",
    sentPrompt: "hello",
    tabId: "tab-7",
    url: "https://chatgpt.com/c/demo",
  });

  assert.equal(task.taskStoreDir, defaultTaskStoreDir);
});

test("startTaskWatcher launches detached watcher and records watcher metadata", () => {
  const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "chatgpt-watch-"));
  const task = createTaskRecord({
    mode: "search",
    prompt: "hello",
    sentPrompt: "wrapped hello",
    tabId: "tab-9",
    url: "https://chatgpt.com/c/demo",
    taskStoreDir: tmpdir,
  });
  saveTaskRecord(task, tmpdir);

  const spawnCalls = [];
  let unrefCalled = false;
  const updated = startTaskWatcher(task.id, {
    dir: tmpdir,
    nodeBin: "node-custom",
    scriptPath: "/tmp/chatgpt_cdp.mjs",
    env: {},
    now: () => "2026-03-23T03:00:00.000Z",
    spawnImpl: (command, args, options) => {
      spawnCalls.push({ command, args, options });
      return {
        pid: 4321,
        unref() {
          unrefCalled = true;
        },
      };
    },
  });

  assert.equal(spawnCalls.length, 1);
  assert.equal(spawnCalls[0].command, "node-custom");
  assert.deepEqual(spawnCalls[0].args, ["/tmp/chatgpt_cdp.mjs", "watch", task.id]);
  assert.equal(spawnCalls[0].options.detached, true);
  assert.equal(spawnCalls[0].options.env.CHATGPT_PLUS_TASK_DIR, tmpdir);
  assert.equal(unrefCalled, true);
  assert.equal(updated.watcherPid, 4321);
  assert.equal(updated.watcherMode, "auto");
  assert.equal(updated.watcherStartedAt, "2026-03-23T03:00:00.000Z");
});

test("startTaskWatcher can be disabled by env without spawning child process", () => {
  const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "chatgpt-watch-off-"));
  const task = createTaskRecord({
    mode: "plain",
    prompt: "hello",
    sentPrompt: "hello",
    tabId: "tab-4",
    url: "https://chatgpt.com/c/demo",
    taskStoreDir: tmpdir,
  });
  saveTaskRecord(task, tmpdir);

  let called = false;
  const updated = startTaskWatcher(task.id, {
    dir: tmpdir,
    env: { CHATGPT_PLUS_DISABLE_AUTO_WATCH: "1" },
    spawnImpl: () => {
      called = true;
      throw new Error("should not spawn");
    },
  });

  assert.equal(called, false);
  assert.equal(updated.watcherMode, undefined);
  assert.equal(updated.watcherPid, undefined);
});

test("startTaskWatcher records watcherError when spawn fails", () => {
  const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "chatgpt-watch-fail-"));
  const task = createTaskRecord({
    mode: "search",
    prompt: "hello",
    sentPrompt: "wrapped hello",
    tabId: "tab-8",
    url: "https://chatgpt.com/c/demo",
    taskStoreDir: tmpdir,
  });
  saveTaskRecord(task, tmpdir);

  const updated = startTaskWatcher(task.id, {
    dir: tmpdir,
    env: {},
    now: () => "2026-03-23T03:10:00.000Z",
    spawnImpl: () => {
      throw new Error("spawn boom");
    },
  });

  assert.equal(updated.watcherError, "spawn boom");
  assert.equal(updated.watcherFinishedAt, "2026-03-23T03:10:00.000Z");
  const loaded = loadTaskRecord(task.id, tmpdir);
  assert.equal(loaded.watcherError, "spawn boom");
});
