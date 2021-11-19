#!/usr/bin/env node
const { spawnSync } = require('child_process')
const spellchecker=__dirname
const spawnArguments={stdio:[process.stdin, process.stdout, process.stderr]}

function run() {
  process.stdout.write("cwd: "+process.cwd()+"\n")
  process.stdout.write("spellchecker: "+spellchecker+"\n")
  process.env['spellchecker'] = spellchecker;

  const child=spawnSync(spellchecker+"/unknown-words.sh", process.argv.slice(1), spawnArguments)
  process.exit(child.status)
}

run()
