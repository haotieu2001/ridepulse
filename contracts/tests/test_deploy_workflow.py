"""The path to the live environment (#78): stg, a smoke on stg, then an approval before anything live changes."""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEPLOY = yaml.safe_load((ROOT / ".github/workflows/deploy.yml").read_text())
CI = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
JOBS = DEPLOY["jobs"]


def steps(job: str) -> str:
    return "\n".join(str(s.get("run", "")) + str(s.get("with", "")) for s in JOBS[job]["steps"])


def test_live_waits_for_stg_its_smoke_and_an_approval():
    assert JOBS["stg_smoke"]["needs"] == "stg" and JOBS["live"]["needs"] == "stg_smoke"
    assert "--validate-only" in steps("stg_smoke")
    assert JOBS["live"]["environment"] == "live"   # a required reviewer, main only (repository settings)


def test_only_the_live_job_holds_the_deploy_role():
    assert "ridepulse-github-deploy" in steps("live")
    for name, job in JOBS.items():
        if name != "live":
            assert "ridepulse-github-deploy" not in str(job), name
    assert "ridepulse-github-deploy" not in str(CI)   # PRs plan with the read-only role


def test_an_apply_that_would_destroy_or_replace_stops():
    live = steps("live")
    assert 'index("delete")' in live and "exit 1" in live
    # The stacks CI applies: not bootstrap (its own roles) nor databricks (a workspace admin's).
    assert "for stack in confluent landing debezium replayer" in live


def test_dev_is_deployed_by_ridepulse_ci_from_a_laptop_too():
    makefile = (ROOT / "Makefile").read_text()
    assert "../scripts/as_ci.sh databricks bundle deploy -t dev" in makefile
