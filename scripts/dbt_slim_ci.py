"""Resolve state-aware dbt slim CI selections."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path


def _run(command: list[str], *, cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        check=True,
        text=True,
        capture_output=True,
    )
    return completed.stdout


def _resolve_profiles_source(profiles_dir: Path) -> Path:
    profiles_file = profiles_dir / "profiles.yml"
    if profiles_file.exists():
        return profiles_file

    example = profiles_dir / "profiles.yml.example"
    if example.exists():
        return example
    raise FileNotFoundError(f"Missing dbt profiles template: {example}")


def _build_baseline_manifest(
    *,
    repo_root: Path,
    temp_root: Path,
    baseline_ref: str,
    project_dir: Path,
    profiles_file: Path,
    dbt_bin: str,
    state_dir: Path,
) -> None:
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dbt-state-", dir=str(temp_root)
    ) as temp_dir:
        worktree_dir = Path(temp_dir) / "baseline"
        _run(
            ["git", "worktree", "add", "--detach", str(worktree_dir), baseline_ref],
            cwd=repo_root,
        )
        try:
            baseline_profiles = worktree_dir / ".tmp" / "dbt_profiles"
            baseline_profiles.mkdir(parents=True, exist_ok=True)
            (baseline_profiles / "profiles.yml").write_text(
                profiles_file.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            _run(
                [
                    dbt_bin,
                    "parse",
                    "--project-dir",
                    str(worktree_dir / project_dir),
                    "--profiles-dir",
                    str(baseline_profiles.resolve()),
                    "--target",
                    "dev",
                ],
                cwd=repo_root,
            )

            baseline_manifest = worktree_dir / project_dir / "target" / "manifest.json"
            if baseline_manifest.exists():
                state_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(baseline_manifest, state_dir / "manifest.json")
        finally:
            _run(
                ["git", "worktree", "remove", "--force", str(worktree_dir)],
                cwd=repo_root,
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve dbt slim CI state selections."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--project-dir", default="warehouse/dbt")
    parser.add_argument("--profiles-dir", default="warehouse/dbt/profiles")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--dbt-bin", default="dbt")
    parser.add_argument("--state-dir", default=".tmp/dbt-state")
    parser.add_argument("--baseline-ref", default="origin/main")
    parser.add_argument("--fallback-selector", default="phase2_governed_models")
    parser.add_argument("--output-file", default=".tmp/dbt-state/selection.txt")
    parser.add_argument("--skip-baseline-build", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if shutil.which(args.dbt_bin) is None:
        raise FileNotFoundError(
            f"dbt executable not found: {args.dbt_bin}. Install requirements-dbt.txt first."
        )

    repo_root = Path(args.repo_root).resolve()
    project_dir = Path(args.project_dir)
    profiles_dir = Path(args.profiles_dir)
    state_dir = Path(args.state_dir)
    output_file = Path(args.output_file)
    temp_root = repo_root / ".tmp"
    manifest_path = state_dir / "manifest.json"

    profiles_source = _resolve_profiles_source(repo_root / profiles_dir)
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dbt-profiles-",
        dir=str(temp_root),
    ) as temp_profiles_dir:
        temp_profiles = Path(temp_profiles_dir)
        shutil.copy2(profiles_source, temp_profiles / "profiles.yml")

        if not manifest_path.exists() and not args.skip_baseline_build:
            _run(["git", "fetch", "origin", "main", "--depth", "1"], cwd=repo_root)
            _build_baseline_manifest(
                repo_root=repo_root,
                temp_root=temp_root,
                baseline_ref=args.baseline_ref,
                project_dir=project_dir,
                profiles_file=temp_profiles / "profiles.yml",
                dbt_bin=args.dbt_bin,
                state_dir=state_dir,
            )

        _run(
            [
                args.dbt_bin,
                "parse",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(temp_profiles.resolve()),
                "--target",
                args.target,
            ],
            cwd=repo_root,
        )

        if manifest_path.exists():
            selection_output = _run(
                [
                    args.dbt_bin,
                    "ls",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(temp_profiles.resolve()),
                    "--target",
                    args.target,
                    "--state",
                    str(state_dir),
                    "--select",
                    "state:modified+",
                    "--output",
                    "name",
                ],
                cwd=repo_root,
            )
        else:
            selection_output = _run(
                [
                    args.dbt_bin,
                    "ls",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(temp_profiles.resolve()),
                    "--target",
                    args.target,
                    "--selector",
                    args.fallback_selector,
                    "--output",
                    "name",
                ],
                cwd=repo_root,
            )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(selection_output.strip() + "\n", encoding="utf-8")
    print(output_file.read_text(encoding="utf-8").strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
