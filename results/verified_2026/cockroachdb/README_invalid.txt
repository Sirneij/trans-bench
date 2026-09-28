runs_invalid_cleanup_bug.jsonl: CockroachDB runs on scale_free (right recursion) and barabasi_albert that
failed with 'table "tc_result" is being added'. They followed the timeout of scale_free/left/20000, whose
cancelled CREATE TABLE ... AS left a schema-change job behind; the driver's post-timeout cleanup did not
wait for it (fixed in trans-bench commit ad14a66b). These configurations were re-run afterwards.
A first re-run attempt (2026-09-27T23:44Z) failed the same way because the CREATE TABLE ... AS of the
timed-out run is executed by CockroachDB as a background schema-change job that kept running (and resumed
after the server restart). The job was cancelled manually (CANCEL JOB 1213804023738073089), the tables were
dropped, and the configurations were re-run.
A second re-run (2026-09-28T01:03Z) of barabasi_albert failed the same way after the timeout of scale_free/right/30000, because the driver's bulk CANCEL JOBS also selected non-cancelable GC jobs and therefore failed (fixed in commit 3fe8df8e); the job was cancelled manually (CANCEL JOB 1214007923699646465) and barabasi_albert was re-run.
