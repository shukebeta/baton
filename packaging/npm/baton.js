#!/usr/bin/env node
'use strict';

const { spawn, spawnSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const platformPackages = {
  'linux-x64': '@shukelabs/baton-linux-x64',
  'linux-arm64': '@shukelabs/baton-linux-arm64',
  'darwin-x64': '@shukelabs/baton-darwin-x64',
  'darwin-arm64': '@shukelabs/baton-darwin-arm64',
  'win32-x64': '@shukelabs/baton-win32-x64',
};

const INSTALL_HINT = 'hint: run "baton install" to put the native binary in ~/.local/bin';

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

function resolvedPackageVersion(packageName) {
  return require(`${packageName}/package.json`).version;
}

function defaultInstallDir(homeDir) {
  return path.join(homeDir, '.local', 'bin');
}

function installedVersion(destPath) {
  if (!fs.existsSync(destPath)) return null;
  const result = spawnSync(destPath, ['--version'], { encoding: 'utf8' });
  if (result.error || result.status !== 0) return null;
  return result.stdout.trim();
}

function parseInstallArgs(argv) {
  let prefix = null;
  let silent = false;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--prefix') {
      prefix = argv[i + 1];
      i += 1;
    } else if (argv[i] === '--silent') {
      silent = true;
    }
  }
  return { prefix, silent };
}

function installNativeBinary(argv, { platform = process.platform, homeDir = os.homedir() } = {}) {
  const { prefix, silent } = parseInstallArgs(argv);

  if (platform === 'win32') {
    console.log(
      'baton: native binary install is not supported on Windows; the service installer handles Windows deployment.',
    );
    return 0;
  }

  if (silent && process.env.npm_config_global !== 'true') {
    return 0;
  }

  let resolved;
  try {
    resolved = resolvePlatformBinary(platform);
  } catch (error) {
    console.error(`baton: ${error.message}`);
    return 1;
  }

  const source = fs.realpathSync(resolved.binaryPath);
  const expectedVersion = `baton ${resolvedPackageVersion(resolved.packageName)}`;
  const destDir = prefix || defaultInstallDir(homeDir);
  const destPath = path.join(destDir, 'baton');

  if (installedVersion(destPath) === expectedVersion) {
    return 0;
  }

  try {
    fs.mkdirSync(destDir, { recursive: true });
    fs.copyFileSync(source, destPath);
    fs.chmodSync(destPath, 0o755);
  } catch (error) {
    if (error.code === 'EACCES') {
      console.error(`baton: permission denied writing ${destPath} — re-run under sudo`);
      return 1;
    }
    throw error;
  }

  if (!silent) {
    console.log(`baton: installed native binary to ${destPath}`);
  }
  return 0;
}

function maybePrintInstallHint({ platform = process.platform, homeDir = os.homedir() } = {}) {
  if (platform === 'win32' || process.env.BATON_NO_INSTALL_HINT) return;

  try {
    const resolved = resolvePlatformBinary(platform);
    const expectedVersion = `baton ${resolvedPackageVersion(resolved.packageName)}`;
    const destPath = path.join(defaultInstallDir(homeDir), 'baton');

    if (installedVersion(destPath) === expectedVersion) return;

    const markerPath = path.join(homeDir, '.local', 'state', 'baton', 'npm-install-hint');
    const lastHinted = fs.existsSync(markerPath) ? fs.readFileSync(markerPath, 'utf8').trim() : null;
    if (lastHinted === expectedVersion) return;

    console.error(INSTALL_HINT);
    fs.mkdirSync(path.dirname(markerPath), { recursive: true });
    fs.writeFileSync(markerPath, expectedVersion);
  } catch {
    // Never let hint bookkeeping block the real command.
  }
}

function main() {
  const args = process.argv.slice(2);

  if (args[0] === 'install') {
    process.exitCode = installNativeBinary(args.slice(1));
    return;
  }

  maybePrintInstallHint();

  let resolved;
  try {
    resolved = resolvePlatformBinary();
  } catch (error) {
    fail(error.message);
  }

  const child = spawn(resolved.binaryPath, args, { stdio: 'inherit' });
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

module.exports = {
  platformPackages,
  resolvePlatformBinary,
  installNativeBinary,
  maybePrintInstallHint,
};
