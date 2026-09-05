#!/usr/bin/env node
'use strict';

const { spawn } = require('child_process');

const platformPackages = {
  'linux-x64': '@shukelabs/baton-linux-x64',
  'linux-arm64': '@shukelabs/baton-linux-arm64',
  'darwin-x64': '@shukelabs/baton-darwin-x64',
  'darwin-arm64': '@shukelabs/baton-darwin-arm64',
  'win32-x64': '@shukelabs/baton-win32-x64',
};

const platform = `${process.platform}-${process.arch}`;
const packageName = platformPackages[platform];
const binaryName = process.platform === 'win32' ? 'baton.exe' : 'baton';

function fail(message) {
  console.error(`baton: ${message}`);
  process.exit(1);
}

if (!packageName) {
  fail(`platform not supported (${process.platform}/${process.arch})`);
}

let binaryPath;
try {
  binaryPath = require.resolve(`${packageName}/bin/${binaryName}`);
} catch {
  fail(`platform package ${packageName} is not installed`);
}

const child = spawn(binaryPath, process.argv.slice(2), { stdio: 'inherit' });
child.on('error', (error) => {
  console.error(`baton: failed to start native binary: ${error.message}`);
  process.exitCode = 1;
});
child.on('exit', (code) => {
  process.exitCode = code === null ? 1 : code;
});
