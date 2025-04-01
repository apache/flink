# Flink Git Hooks

This folder contains useful git hooks for the Flink project.

## Usage

You can copy the hooks from this folder to your local `.git/hooks` directory. 
However, symbolic links are preferred, as they mean you will automatically get any updates to the hooks when you pull from the repository.

From the repository root, run the following command to create a symbolic link for the pre-commit hook:

```shell
ln -s ../../tools/git-hooks/pre-commit .git/hooks/pre-commit
```
