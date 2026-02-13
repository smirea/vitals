import fs from 'fs';
import path from 'path';

import { createScript } from './createScript.ts';

type PerfRunResult = {
    selectMs: number;
    unselectMs: number;
    ungroupMs: number;
    regroupMs: number;
    shortFilterMs: number;
    clearFilterMs: number;
    longTaskMaxMs: number;
    longTaskTotalMs: number;
};

type PerfSummary = {
    runs: number;
    threshold: number;
    medians: PerfRunResult;
    p90: PerfRunResult;
    max: PerfRunResult;
    pass: boolean;
};

const PERF_PROBE_CODE = `async page => {
  await page.reload();

  const now = () => (globalThis.performance?.now?.() ?? Date.now());
  const waitForPaint = async () => {
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  };

  const rowSelector = '.vitals-table tbody tr:not(.vitals-category-row)';
  await page.waitForFunction(selector => document.querySelectorAll(selector).length > 0, rowSelector, { timeout: 10000 });

  await page.evaluate(() => {
    const existing = window.__vitalsLongTaskObserver;
    if (existing) existing.disconnect();
    window.__vitalsLongTasks = [];

    if (typeof PerformanceObserver !== 'undefined') {
      try {
        const observer = new PerformanceObserver(list => {
          const entries = list.getEntries().map(entry => ({
            startTime: entry.startTime,
            duration: entry.duration,
            name: entry.name,
            entryType: entry.entryType,
          }));
          window.__vitalsLongTasks.push(...entries);
        });
        observer.observe({ entryTypes: ['longtask'] });
        window.__vitalsLongTaskObserver = observer;
      } catch {
        window.__vitalsLongTaskObserver = null;
      }
    }
  });

  const firstCheckbox = page.locator('.vitals-table tbody tr:not(.vitals-category-row) .vitals-col-select input[type="checkbox"]').first();
  const groupCheckbox = page.getByRole('checkbox', { name: 'Group by category' });
  const filterInput = page.getByPlaceholder('Filter measurements');

  if (!(await groupCheckbox.isChecked())) {
    await groupCheckbox.check({ force: true });
    await page.waitForTimeout(80);
  }

  const tSelectStart = now();
  await firstCheckbox.click({ force: true });
  await page.waitForFunction(() => {
    const node = document.querySelector('.vitals-table tbody tr:not(.vitals-category-row) .vitals-col-select input[type="checkbox"]');
    return node instanceof HTMLInputElement && node.checked;
  }, undefined, { timeout: 6000 });
  await waitForPaint();
  const tSelectEnd = now();

  const tUnselectStart = now();
  await firstCheckbox.click({ force: true });
  await page.waitForFunction(() => {
    const node = document.querySelector('.vitals-table tbody tr:not(.vitals-category-row) .vitals-col-select input[type="checkbox"]');
    return node instanceof HTMLInputElement && !node.checked;
  }, undefined, { timeout: 6000 });
  await waitForPaint();
  const tUnselectEnd = now();

  const rowsBeforeUngroup = await page.locator('.vitals-table tbody tr').count();
  const tUngroupStart = now();
  await groupCheckbox.uncheck({ force: true });
  await page.waitForFunction(previous => {
    return document.querySelectorAll('.vitals-table tbody tr').length !== previous;
  }, rowsBeforeUngroup, { timeout: 6000 });
  await waitForPaint();
  const tUngroupEnd = now();

  const rowsBeforeRegroup = await page.locator('.vitals-table tbody tr').count();
  const tRegroupStart = now();
  await groupCheckbox.check({ force: true });
  await page.waitForFunction(previous => {
    return document.querySelectorAll('.vitals-table tbody tr').length !== previous;
  }, rowsBeforeRegroup, { timeout: 6000 });
  await waitForPaint();
  const tRegroupEnd = now();

  const tShortFilterStart = now();
  await filterInput.fill('a');
  await page.waitForTimeout(120);
  await waitForPaint();
  const tShortFilterEnd = now();

  const tClearFilterStart = now();
  await filterInput.fill('');
  await page.waitForTimeout(120);
  await waitForPaint();
  const tClearFilterEnd = now();

  const longTasks = await page.evaluate(() => {
    const entries = window.__vitalsLongTasks || [];
    return {
      max: entries.reduce((acc, entry) => Math.max(acc, entry.duration || 0), 0),
      total: entries.reduce((acc, entry) => acc + (entry.duration || 0), 0),
    };
  });

  return {
    selectMs: tSelectEnd - tSelectStart,
    unselectMs: tUnselectEnd - tUnselectStart,
    ungroupMs: tUngroupEnd - tUngroupStart,
    regroupMs: tRegroupEnd - tRegroupStart,
    shortFilterMs: tShortFilterEnd - tShortFilterStart,
    clearFilterMs: tClearFilterEnd - tClearFilterStart,
    longTaskMaxMs: longTasks.max,
    longTaskTotalMs: longTasks.total,
  };
}`;

function parseArgs(argv: string[]) {
    const values: Record<string, string> = {};

    for (let index = 0; index < argv.length; index += 1) {
        const token = argv[index];
        if (!token.startsWith('--')) continue;

        const maybeValue = argv[index + 1];
        if (!maybeValue || maybeValue.startsWith('--')) {
            values[token.slice(2)] = 'true';
            continue;
        }

        values[token.slice(2)] = maybeValue;
        index += 1;
    }

    return {
        url: values.url ?? 'http://localhost:3000',
        runs: Number(values.runs ?? 10),
        threshold: Number(values.threshold ?? 250),
        session: values.session ?? `vitals-perf-${Date.now()}`,
        jsonOnly: values.json === 'true',
    };
}

function runPwCli({
    pwcli,
    session,
    args,
}: {
    pwcli: string;
    session: string;
    args: string[];
}): string {
    const command = [pwcli, '--session', session, ...args];
    const proc = Bun.spawnSync(command, {
        stdout: 'pipe',
        stderr: 'pipe',
        stdin: 'ignore',
        cwd: process.cwd(),
        env: process.env,
    });

    const stdout = proc.stdout.toString();
    const stderr = proc.stderr.toString();

    if (proc.exitCode !== 0) {
        throw new Error([
            `Playwright CLI command failed: ${command.join(' ')}`,
            stdout.trim(),
            stderr.trim(),
        ].filter(Boolean).join('\n'));
    }

    return stdout;
}

function parseResultJson(output: string): PerfRunResult {
    const match = output.match(/### Result\n([\s\S]*?)\n### Ran Playwright code/);
    if (!match?.[1]) {
        throw new Error(`Could not parse result from Playwright output:\n${output}`);
    }

    return JSON.parse(match[1]) as PerfRunResult;
}

function median(values: number[]): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((left, right) => left - right);
    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
        return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
}

function percentile(values: number[], pct: number): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((left, right) => left - right);
    const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((pct / 100) * sorted.length) - 1));
    return sorted[index] ?? 0;
}

function maxValue(values: number[]): number {
    return values.length > 0 ? Math.max(...values) : 0;
}

function summarize(results: PerfRunResult[], threshold: number): PerfSummary {
    const metricKeys = Object.keys(results[0] ?? {
        selectMs: 0,
        unselectMs: 0,
        ungroupMs: 0,
        regroupMs: 0,
        shortFilterMs: 0,
        clearFilterMs: 0,
        longTaskMaxMs: 0,
        longTaskTotalMs: 0,
    }) as Array<keyof PerfRunResult>;

    const medians = {} as PerfRunResult;
    const p90 = {} as PerfRunResult;
    const max = {} as PerfRunResult;

    for (const key of metricKeys) {
        const values = results.map(result => result[key]);
        medians[key] = Number(median(values).toFixed(1));
        p90[key] = Number(percentile(values, 90).toFixed(1));
        max[key] = Number(maxValue(values).toFixed(1));
    }

    const gatedKeys: Array<keyof PerfRunResult> = [
        'selectMs',
        'unselectMs',
        'regroupMs',
        'shortFilterMs',
        'clearFilterMs',
    ];

    const pass = gatedKeys.every(key => medians[key] < threshold);

    return {
        runs: results.length,
        threshold,
        medians,
        p90,
        max,
        pass,
    };
}

function printSummary(summary: PerfSummary) {
    const lines = [
        `Perf summary (${summary.runs} runs, threshold ${summary.threshold}ms):`,
        `  median  select=${summary.medians.selectMs}  unselect=${summary.medians.unselectMs}  regroup=${summary.medians.regroupMs}  shortFilter=${summary.medians.shortFilterMs}  clearFilter=${summary.medians.clearFilterMs}`,
        `  p90     select=${summary.p90.selectMs}  unselect=${summary.p90.unselectMs}  regroup=${summary.p90.regroupMs}  shortFilter=${summary.p90.shortFilterMs}  clearFilter=${summary.p90.clearFilterMs}`,
        `  longTask median max=${summary.medians.longTaskMaxMs} total=${summary.medians.longTaskTotalMs}`,
        `  result: ${summary.pass ? 'PASS' : 'FAIL'}`,
    ];

    for (const line of lines) {
        console.log(line);
    }
}

async function ensureUrlIsReachable(url: string) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);

    try {
        const response = await fetch(url, {
            method: 'GET',
            signal: controller.signal,
        });
        if (!response.ok) {
            throw new Error(`Received HTTP ${response.status}`);
        }
    } finally {
        clearTimeout(timeout);
    }
}

await createScript(async () => {
    const args = parseArgs(process.argv.slice(2));

    if (!Number.isFinite(args.runs) || args.runs <= 0) {
        throw new Error(`Invalid --runs value: ${args.runs}`);
    }

    if (!Number.isFinite(args.threshold) || args.threshold <= 0) {
        throw new Error(`Invalid --threshold value: ${args.threshold}`);
    }

    const codexHome = process.env.CODEX_HOME ?? path.join(process.env.HOME ?? '', '.codex');
    const pwcli = path.join(codexHome, 'skills', 'playwright', 'scripts', 'playwright_cli.sh');

    if (!fs.existsSync(pwcli)) {
        throw new Error(`Playwright CLI wrapper not found at ${pwcli}`);
    }

    await ensureUrlIsReachable(args.url);

    runPwCli({
        pwcli,
        session: args.session,
        args: ['open', args.url],
    });

    const runResults: PerfRunResult[] = [];

    try {
        for (let index = 0; index < args.runs; index += 1) {
            const output = runPwCli({
                pwcli,
                session: args.session,
                args: ['run-code', PERF_PROBE_CODE],
            });
            const parsed = parseResultJson(output);
            runResults.push(parsed);
            if (!args.jsonOnly) {
                const runId = String(index + 1).padStart(2, '0');
                console.log(`run ${runId}: select=${parsed.selectMs.toFixed(1)} regroup=${parsed.regroupMs.toFixed(1)} filter=${parsed.shortFilterMs.toFixed(1)} clear=${parsed.clearFilterMs.toFixed(1)} longTaskMax=${parsed.longTaskMaxMs.toFixed(1)}`);
            }
        }
    } finally {
        runPwCli({
            pwcli,
            session: args.session,
            args: ['close'],
        });
    }

    const summary = summarize(runResults, args.threshold);

    if (args.jsonOnly) {
        console.log(JSON.stringify(summary, null, 2));
    } else {
        printSummary(summary);
    }

    if (!summary.pass) {
        throw new Error(`Performance gate failed: median metrics exceeded ${args.threshold}ms threshold`);
    }
});
