const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");

function view() {
  function element() {
    return {
      hidden: true, textContent: "", children: [],
      replaceChildren() { this.children = []; },
      append(...children) { this.children.push(...children); },
    };
  }
  const elements = Object.fromEntries(
    ["upload-timings", "upload-timing-values", "upload-timing-summary"].map(id => [id, element()]),
  );
  const context = {
    window: {},
    document: { getElementById: id => elements[id], createElement: element },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../ui/static/upload-timings.js"), "utf8"), context);
  return { elements, api: context.window.UploadTimingView };
}

test("cached old API responses without metrics remain usable", () => {
  const { elements, api } = view();
  api.render(undefined);
  assert.equal(elements["upload-timings"].hidden, true);
});

test("live phase appears once and completion clears its active marker", () => {
  const { elements, api } = view();
  const timing = { phase: "transferring", totalSeconds: 12.345, seconds: { preparing: 2, transferring: 10.345 } };
  api.render(timing);
  assert.equal(elements["upload-timings"].hidden, false);
  assert.match(elements["upload-timing-summary"].textContent, /12\.35/);
  const values = elements["upload-timing-values"];
  assert.equal(values.children.length, 12);
  assert.equal(values.children[5].textContent, "0.00 秒");
  assert.equal(values.children[7].textContent, "2.00 秒");
  assert.equal(values.children[11].textContent, "10.35 秒（進行中）");
  api.render({ ...timing, phase: null });
  assert.equal(values.children.length, 12);
  assert.equal(values.children[11].textContent, "10.35 秒");
  api.reset();
  assert.equal(elements["upload-timings"].hidden, true);
});
