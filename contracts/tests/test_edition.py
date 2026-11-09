"""The edition flag (#80): the stacks that differ between Free Edition and the paid design say so the same way."""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
STACKS = ["confluent", "debezium", "landing"]


def tf(stack: str) -> str:
    return "\n".join(p.read_text() for p in sorted((ROOT / "infra/terraform" / stack).glob("*.tf")))


def test_every_stack_that_differs_has_the_same_flag():
    for stack in STACKS:
        block = re.search(r'variable "edition" \{(.*?)\n\}', tf(stack), re.S)
        assert block, stack
        assert 'default     = "free"' in block.group(1), stack
        assert 'contains(["free", "paid"], var.edition)' in block.group(1), stack


def test_the_free_edition_is_what_runs():
    # This project runs the free edition: nothing sets the flag, so every stack takes the default.
    for stack in STACKS:
        for path in (ROOT / "infra/terraform" / stack).glob("*.tfvars"):
            assert "edition" not in path.read_text(), path


def test_paid_cdc_says_it_is_not_built():
    assert 'resource "terraform_data" "paid_cdc"' in tf("debezium") and "ADR-0007" in tf("debezium")
