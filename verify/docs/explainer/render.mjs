// Render index.html to a GIF and a contact sheet: seek each frame, screenshot it, then ffmpeg.
// Needs Node 18+, Playwright with Chromium, and ffmpeg on the PATH.
//   node render.mjs [--fps 8] [--width 960] [--from 0] [--to 54]
// If Playwright is installed elsewhere: NODE_PATH=/path/to/node_modules node render.mjs
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { mkdirSync, rmSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const here = dirname(fileURLToPath(import.meta.url));
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? Number(process.argv[i + 1]) : d; };
const fps = arg('fps', 8), width = arg('width', 960);
const frames = join(here, '.frames');
rmSync(frames, { recursive: true, force: true }); mkdirSync(frames);

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1200, height: 675 }, deviceScaleFactor: 1 });
await page.goto(pathToFileURL(join(here, 'index.html')).href + '?render=1');
await page.evaluate(() => document.fonts.ready);
const dur = await page.evaluate(() => window.__duration);
const from = arg('from', 0), to = arg('to', dur);
const svg = await page.$('#stage');
let n = 0;
for (let t = from; t <= to + 1e-9; t += 1 / fps) {
  await page.evaluate(x => window.__seek(x), t);
  await svg.screenshot({ path: join(frames, `f${String(n++).padStart(4, '0')}.png`) });
}
await browser.close();
console.log(`${n} frames at ${fps} fps`);

const docs = dirname(here);
const gif = join(docs, 'verify-explainer.gif');
const ff = (...a) => execFileSync('ffmpeg', ['-v', 'error', '-y', ...a], { stdio: 'inherit' });
ff('-framerate', String(fps), '-i', join(frames, 'f%04d.png'), '-vf',
   `scale=${width}:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=48:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle`,
   '-loop', '0', gif);
// contact sheet: 8 frames across the whole run, the "check the sheet" step of the skill
const step = Math.max(1, Math.floor(n / 8));
ff('-framerate', String(fps), '-i', join(frames, 'f%04d.png'), '-vf',
   `select='not(mod(n\\,${step}))',scale=300:-1,tile=4x2`, '-frames:v', '1', join(here, '.frames', 'sheet.jpg'));
console.log('wrote', gif, existsSync(gif) ? '' : '(missing)');
