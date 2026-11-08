# ADR-0011: Infrastructure as code, and where each thing is defined

- Status: accepted
- Date: 2026-11-08

## Context

Here, three tools define the platform:

- **Terraform** for AWS and Confluent, in five stacks with state in S3 (`infra/terraform`): bootstrap, landing,
  confluent, debezium, replayer. CI plans three of them with a read-only role (#38).
- **Databricks Asset Bundles** for pipelines, jobs, the dashboard, monitors, and schema and volume grants
  (`databricks/`).
- **Code and SQL** for what neither can reach on Free Edition:
  - Catalogs are made with SQL (Default Storage only, ADR-0001).
  - The gold build applies Unity Catalog grants, the ABAC policy and the row filter (#38).
  - The account groups, their workspace assignment and the `pii` tag policy were made by hand through the
    workspace's account API (`docs/security.md`), because there is no account console.

What went wrong because of the split:

- A Terraform plan once wanted to replace the Schema Registry keys and the ping schema, because a data source
  re-read when the cluster changed (#39). Only a targeted apply avoided it.
- The bootstrap stack was applied from a branch, so it ran ahead of `main` until that PR (#103) merged.
- The groups exist only in the workspace, not in any file.

## Decision

**In production, one rule: Terraform for what lives long and is shared, bundles for what ships with code.**

| Defined in | What |
|---|---|
| Terraform, account level | workspaces per environment, the metastore and its assignment, storage credentials and external locations, network (VPC, PrivateLink, NCCs), account groups synced from the identity provider by SCIM, service principals and their workload identity federation |
| Terraform, workspace level | catalogs and schemas, Unity Catalog grants, tag policies and ABAC policies, cluster policies, SQL warehouses and their permissions, secret scopes |
| Bundles | pipelines, jobs, dashboards, monitors, and the code they run: one bundle, deployed per environment by CI (#78) |
| Terraform, AWS and Confluent | as here: one stack per lifecycle, state in S3 with locking, plans on every PR, applies behind an approval |

**Rules that come with it:**
1. Nothing is made by hand. A drift check (a scheduled plan) alerts when the live system differs from code.
2. Grants live in Terraform, not in the gold build. A table's grants then exist before the first build writes it.
3. A plan that replaces something stateful (a key, a schema, a slot, a table) blocks the apply until a person
   approves that resource by name. This is the `-target` lesson from #39, made a rule.

## Consequences

- One place to review access: a grant is a diff in a PR.
- Bundles stay fast to iterate on, because they hold no long-lived shared objects.
- More Terraform to own, and an account-level identity for it. #77 starts with the service principals and the
  groups made by hand for #38.

Revisit if Databricks' Terraform and bundle coverage converge (bundles managing catalogs and grants), which would
allow one tool for the workspace.
