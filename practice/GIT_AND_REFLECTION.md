# C2: Three real Git loops

Do not mark this task complete until three pull requests have actually been merged. Previously merged PRs can count if they are the three loops you want to document; record their links below.

Before starting, run `git status` and `git diff --cached --stat`. You previously staged several review files. Do not discard those changes or blindly commit all staged files under an unrelated description. Finish that checkpoint commit on a review branch first, or carefully unstage only the paths you want in later commits (unstaging keeps your edits).

For each loop, start from an up-to-date main branch with a clean working tree:

```bash
git switch main
git pull --ff-only origin main
git switch -c your-new-branch-name
```

Make one meaningful change, then stage its exact files and review them:

```bash
git add path/to/changed_file
git diff --cached
git commit -m "Describe the change and its purpose"
git push -u origin your-new-branch-name
```

Open the GitHub link printed by push, create a pull request into main, review the changes and merge. Then:

```bash
git switch main
git pull --ff-only origin main
git status
```

If Ubuntu HTTPS authentication still fails, use your already authenticated PowerShell Git setup for the push, as you did earlier. Never paste passwords or tokens into notes.

Suggested meaningful loops if you still need three:

1. Add the split sessions and retire old scripts after confirming the replacements work.
2. Add the reviewed SQL queries, null policy and regression tests.
3. Update documentation with your actual verification results and reflection.

Copy/stage only the files for the current loop; keep the other downloaded files outside the repo until needed. Do not make three empty commits simply to count them as practice.

| Loop | Branch | PR link | Merged and main pulled? |
| --- | --- | --- | --- |
| 1 | | | |
| 2 | | | |
| 3 | | | |

# C3: Reflection after running the exercises

The end of `notes.md` contains four prompts. Replace them with your own answers after completing the review. Do not claim you understand a query simply because the provided solution runs.
