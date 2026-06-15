#!/usr/bin/env node
import { LogGenerator, presetSchemas } from "@tinybirdco/mockingbird";
import { setTimeout as sleep } from "node:timers/promises";

const DEFAULT_TEMPLATE = "Web Analytics Starter Kit";

function parseArgs(argv) {
  const parsed = new Map();

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected positional argument: ${arg}`);
    }

    const [rawName, inlineValue] = arg.slice(2).split("=", 2);
    const name = rawName.trim();

    if (!name) {
      throw new Error(`Invalid argument: ${arg}`);
    }

    if (inlineValue !== undefined) {
      parsed.set(name, inlineValue);
      continue;
    }

    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      parsed.set(name, true);
      continue;
    }

    parsed.set(name, next);
    index += 1;
  }

  return parsed;
}

function printHelp() {
  console.error(`Usage: node tools/mockingbird-jsonl.mjs [options]\n\nOptions:\n  --template <name>       Mockingbird preset template to use.\n                          Default: ${DEFAULT_TEMPLATE}\n  --eps <number>          Events per second to print. Default: 1\n  --limit <number>        Number of events to print. Use -1 for endless. Default: 10\n  --list-templates        Print available preset template names and exit.\n  --help                  Show this help.\n\nExamples:\n  node tools/mockingbird-jsonl.mjs --limit 3\n  node tools/mockingbird-jsonl.mjs --template \"Stock Prices\" --eps 2 --limit 10`);
}

function parseNumber(value, name, defaultValue) {
  if (value === undefined || value === true) {
    return defaultValue;
  }

  const numberValue = Number(value);
  if (!Number.isFinite(numberValue)) {
    throw new Error(`--${name} must be a number; received ${value}`);
  }

  return numberValue;
}

async function main() {
  process.stdout.on("error", (error) => {
    if (error.code === "EPIPE") {
      process.exit(0);
    }
    throw error;
  });

  const args = parseArgs(process.argv.slice(2));
  const templateNames = Object.keys(presetSchemas).sort();

  if (args.has("help")) {
    printHelp();
    return;
  }

  if (args.has("list-templates")) {
    for (const templateName of templateNames) {
      console.log(templateName);
    }
    return;
  }

  const template = String(args.get("template") ?? DEFAULT_TEMPLATE);
  const schema = presetSchemas[template];

  if (!schema) {
    throw new Error(
      `Unknown Mockingbird template: ${template}\nAvailable templates:\n- ${templateNames.join("\n- ")}`,
    );
  }

  const eps = parseNumber(args.get("eps"), "eps", 1);
  const limit = Math.trunc(parseNumber(args.get("limit"), "limit", 10));

  if (eps <= 0) {
    throw new Error("--eps must be greater than 0");
  }

  if (limit === 0 || limit < -1) {
    throw new Error("--limit must be -1 for endless or a positive integer");
  }

  const generator = new LogGenerator({ schema, eps: 1, limit: 1, logs: false });
  const delayMs = 1000 / eps;
  let emitted = 0;

  while (limit === -1 || emitted < limit) {
    const row = generator.generateRow();
    process.stdout.write(`${JSON.stringify(row)}\n`);
    emitted += 1;

    if (limit === -1 || emitted < limit) {
      await sleep(delayMs);
    }
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
