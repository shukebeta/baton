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

function fail(message) {
  console.error(`baton: ${message}`);
  process.exit(1);
}

function resolvePlatformBinary(platform = process.platform, architecture = process.arch) {
  const platformKey = `${platform}-${architecture}`;
  const packageName = platformPackages[platformKey];
  const binaryName = platform === 'win32' ? 'baton.exe' : 'baton';

  if (!packageName) {
    throw new Error(`platform not supported (${platform}/${architecture})`);
  }

  try {
    return {
      packageName,
      binaryPath: require.resolve(`${packageName}/bin/${binaryName}`),
    };
  } catch {
    throw new Error(`platform package ${packageName} is not installed`);
  }
}

function main() {
  let resolved;
  try {
    resolved = resolvePlatformBinary();
  } catch (error) {
    fail(error.message);
  }

  const child = spawn(resolved.binaryPath, process.argv.slice(2), { stdio: 'inherit' });
  child.on('error', (error) => {
    console.error(`baton: failed to start native binary: ${error.message}`);
    process.exitCode = 1;
  });
  child.on('exit', (code) => {
    process.exitCode = code === null ? 1 : code;
  });
}

if (require.main === module) {
  main();
}

module.exports = { platformPackages, resolvePlatformBinary };
