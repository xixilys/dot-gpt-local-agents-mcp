import { readdir } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// This JavaScript project has no TypeScript build. Check every executable source.
for (const dir of ['src', 'bin', 'scripts', 'test']) {
  for (const file of await readdir(new URL(`../${dir}/`, import.meta.url))) {
    if (!/\.(?:m?js)$/.test(file)) continue;
    const checked = spawnSync(process.execPath, ['--check', fileURLToPath(new URL(`../${dir}/${file}`, import.meta.url))], { stdio: 'inherit' });
    if (checked.status !== 0) process.exit(checked.status ?? 1);
  }
}
