#!/usr/bin/env python3
"""
Strip type annotations from function and lambda parameter lists across the codebase,
and inline function parameter lists (place parameters on a single line).

This script preserves return type annotations and default values while removing
only parameter annotations. Formatting and comments are preserved via LibCST.

Usage:
  python scripts/strip_param_annotations.py [root_dir]

If no root_dir is supplied, the repository root (parent of the scripts directory)
is used.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable, List

import libcst as cst
from libcst import MetadataWrapper


def _make_param_inline(p, keep_trailing_comma) -> cst.Param:
    """Create a new Param derived from p with no annotation and inline whitespace."""
    comma = (
        cst.Comma(
            whitespace_before=cst.SimpleWhitespace(""),
            whitespace_after=cst.SimpleWhitespace(" "),
        )
        if keep_trailing_comma
        else None
    )
    return cst.Param(
        name=p.name,
        annotation=None,
        equal=p.equal if p.equal is not cst.MaybeSentinel.DEFAULT else cst.MaybeSentinel.DEFAULT,
        default=p.default,
        comma=comma,
        star=p.star,
        whitespace_after_star=cst.SimpleWhitespace(""),
        whitespace_after_param=cst.SimpleWhitespace("")
    )


class StripAndInlineParams(cst.CSTTransformer):
    """Remove parameter annotations and force inline parameter lists for functions."""

    def leave_FunctionDef(self, original_node, updated_node) -> cst.FunctionDef:  # noqa: N802
        P = updated_node.params

        pos = list(P.posonly_params)
        par = list(P.params)
        kwonly = list(P.kwonly_params)

        has_star_only = isinstance(P.star_arg, cst.ParamStar)
        has_star_arg = isinstance(P.star_arg, cst.Param)
        has_star_kwarg = P.star_kwarg is not None

        # helpers
        def add_params(seq, next_exists):
            out = []
            for i, param in enumerate(seq):
                keep_comma = i < len(seq) - 1 or next_exists
                out.append(_make_param_inline(param, keep_comma))
            return out

        next_after_pos = bool(par) or has_star_only or has_star_arg or bool(kwonly) or has_star_kwarg
        new_pos = add_params(pos, next_after_pos)

        next_after_par = has_star_only or has_star_arg or bool(kwonly) or has_star_kwarg
        new_par = add_params(par, next_after_par)

        # star section
        new_star_arg = None
        if has_star_only:
            # comma after '*' only if something follows
            star_comma_needed = bool(kwonly) or has_star_kwarg
            star_comma = (
                cst.Comma(
                    whitespace_before=cst.SimpleWhitespace(""),
                    whitespace_after=cst.SimpleWhitespace(" "),
                )
                if star_comma_needed
                else None
            )
            new_star_arg = cst.ParamStar(comma=star_comma)
        elif has_star_arg:
            # *args behaves like a normal Param with star='*'
            keep = bool(kwonly) or has_star_kwarg
            new_star_arg = _make_param_inline(P.star_arg, keep)

        next_after_kw = has_star_kwarg
        new_kw = add_params(kwonly, next_after_kw)

        new_star_kwarg = None
        if has_star_kwarg:
            new_star_kwarg = _make_param_inline(P.star_kwarg, False)

        new_params = cst.Parameters(
            posonly_params=tuple(new_pos),
            posonly_ind=P.posonly_ind,
            params=tuple(new_par),
            star_arg=new_star_arg,
            kwonly_params=tuple(new_kw),
            star_kwarg=new_star_kwarg,
        )

        # Force the parenthesis to start immediately after the name (inline)
        return updated_node.with_changes(
            params=new_params,
            whitespace_before_params=cst.SimpleWhitespace("")
        )

    def leave_Param(self, original_node, updated_node) -> cst.Param:  # noqa: N802
        # Also strip annotations in any other contexts (e.g., lambdas)
        if updated_node.annotation is not None:
            return updated_node.with_changes(annotation=None)
        return updated_node


def find_python_files(root) -> Iterable[Path]:
    excluded_dirs = {".git", ".hg", ".svn", "node_modules", ".venv", "venv", "__pycache__"}
    for dirpath, dirnames, filenames in os.walk(root):
        # prune excluded directories
        dirnames[:] = [d for d in dirnames if d not in excluded_dirs]
        for filename in filenames:
            if filename.endswith(".py"):
                yield Path(dirpath) / filename


def rewrite_file(path) -> bool:
    source = path.read_text(encoding="utf-8")
    try:
        module = cst.parse_module(source)
    except Exception:
        # Skip files that cannot be parsed
        return False
    wrapper = MetadataWrapper(module)
    transformed = wrapper.module.visit(StripAndInlineParams())
    new_code = transformed.code
    if new_code != source:
        path.write_text(new_code, encoding="utf-8")
        return True
    return False


def main(argv) -> int:
    if len(argv) > 1:
        root = Path(argv[1]).resolve()
    else:
        root = Path(__file__).resolve().parents[1]

    changed: List[Path] = []
    for py_file in find_python_files(root):
        # Limit scope to project directories we own
        relative = py_file.relative_to(root)
        top_level = relative.parts[0] if relative.parts else ""
        if top_level not in {"app", "tests", "scripts"}:
            continue
        if rewrite_file(py_file):
            changed.append(py_file)

    if changed:
        print("Updated parameter annotations/formatting in:")
        for p in changed:
            print(str(p))
    else:
        print("No changes needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


