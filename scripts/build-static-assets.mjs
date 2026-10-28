import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { build } from "esbuild";

const vendorDir = "static/vendor";
const fontsDir = "static/fonts";

await Promise.all([
    mkdir(vendorDir, { recursive: true }),
    mkdir(fontsDir, { recursive: true }),
]);

await build({
    entryPoints: ["static/src/lucide.js"],
    outfile: `${vendorDir}/lucide.min.js`,
    bundle: true,
    minify: true,
    format: "iife",
    platform: "browser",
    target: ["es2018"],
    legalComments: "none",
});

await copyFile(
    "node_modules/apexcharts/dist/apexcharts.min.js",
    `${vendorDir}/apexcharts.min.js`,
);

const fontFiles = [
    ["@fontsource/barlow/files/barlow-latin-400-normal.woff2", "barlow-latin-400-normal.woff2"],
    ["@fontsource/barlow/files/barlow-latin-500-normal.woff2", "barlow-latin-500-normal.woff2"],
    ["@fontsource/barlow/files/barlow-latin-600-normal.woff2", "barlow-latin-600-normal.woff2"],
    ["@fontsource/barlow/files/barlow-latin-700-normal.woff2", "barlow-latin-700-normal.woff2"],
    ["@fontsource/barlow/files/barlow-latin-800-normal.woff2", "barlow-latin-800-normal.woff2"],
    ["@fontsource/space-mono/files/space-mono-latin-400-normal.woff2", "space-mono-latin-400-normal.woff2"],
    ["@fontsource/space-mono/files/space-mono-latin-700-normal.woff2", "space-mono-latin-700-normal.woff2"],
];

await Promise.all(
    fontFiles.map(([source, destination]) =>
        copyFile(`node_modules/${source}`, `${fontsDir}/${destination}`),
    ),
);

const licenses = [
    ["ApexCharts 3.54.1 (MIT)", "node_modules/apexcharts/LICENSE"],
    ["Lucide 1.63.0 (ISC)", "node_modules/lucide/LICENSE"],
    ["Barlow (SIL Open Font License 1.1)", "node_modules/@fontsource/barlow/LICENSE"],
    ["Space Mono (SIL Open Font License 1.1)", "node_modules/@fontsource/space-mono/LICENSE"],
];

const licenseSections = await Promise.all(
    licenses.map(async ([name, path]) => {
        const license = await readFile(path, "utf8");
        return `## ${name}\n\n${license.trim()}`;
    }),
);

await writeFile(`${vendorDir}/LICENSES.md`, `${licenseSections.join("\n\n")}\n`);
