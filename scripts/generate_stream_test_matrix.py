import ast
import csv
from pathlib import Path


def _get_class(module_node, class_name):
    for node in module_node.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    raise ValueError(f"Class {class_name} not found")


def _function_return_dict(class_node, function_name):
    for node in class_node.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            for statement in node.body:
                if isinstance(statement, ast.Return) and isinstance(statement.value, ast.Dict):
                    return statement.value
    raise ValueError(f"Function {function_name} with dict return not found")


def _set_assignment(class_node, name):
    for node in class_node.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.Set):
                        return {
                            element.value
                            for element in node.value.elts
                            if isinstance(element, ast.Constant) and isinstance(element.value, str)
                        }
    raise ValueError(f"Set assignment {name} not found")


def _stream_exclusions(class_node):
    return_dict = _function_return_dict(class_node, "stream_exclusion_reasons")
    exclusions = set()
    for key in return_dict.keys:
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            exclusions.add(key.value)
    return exclusions


def _extract_metadata(base_path):
    module = ast.parse(base_path.read_text(encoding="utf-8"))
    cls = _get_class(module, "QualtricsBaseTest")

    metadata_dict = _function_return_dict(cls, "expected_metadata")
    stream_metadata = {}
    for key_node, value_node in zip(metadata_dict.keys, metadata_dict.values):
        if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
            continue
        stream_name = key_node.value
        obeys_start_date = False
        if isinstance(value_node, ast.Dict):
            for inner_key, inner_value in zip(value_node.keys, value_node.values):
                if isinstance(inner_key, ast.Attribute) and inner_key.attr == "OBEYS_START_DATE":
                    if isinstance(inner_value, ast.Constant) and isinstance(inner_value.value, bool):
                        obeys_start_date = inner_value.value
                    break
        stream_metadata[stream_name] = {"obeys_start_date": obeys_start_date}

    pr_tier = _set_assignment(cls, "_STREAMS_FOR_TESTING_PR")
    nightly_tier = _set_assignment(cls, "_STREAMS_FOR_TESTING_NIGHTLY")
    exclusions = _stream_exclusions(cls)

    return stream_metadata, pr_tier, nightly_tier, exclusions


def _rows(stream_metadata, pr_tier, nightly_tier, exclusions):
    effective_pr = pr_tier - exclusions
    effective_nightly = nightly_tier - exclusions

    no_ticket = {"tickets"}
    no_interrupted = {"users", "groups", "tickets"}
    pagination_streams = {"users", "groups", "surveys", "mailing_lists"}
    auth_negative_streams = {"users", "surveys"}
    bookmark_streams = {"mailing_lists"}

    rows = []
    for stream in sorted(stream_metadata):
        in_pr = stream in effective_pr
        in_nightly = stream in effective_nightly
        obeys_start_date = stream_metadata[stream]["obeys_start_date"]

        row = {
            "stream": stream,
            "in_pr_tier": in_pr,
            "in_nightly_tier": in_nightly,
            "discovery": in_nightly,
            "all_fields": in_nightly and stream not in no_ticket,
            "automatic_fields": in_nightly and stream not in no_ticket,
            "interrupted_sync": in_nightly and stream not in no_interrupted,
            "pagination": in_nightly and stream in pagination_streams,
            "bookmark": in_nightly and stream in bookmark_streams,
            "start_date": in_nightly and obeys_start_date,
            "auth_config_negative": stream in auth_negative_streams,
            "contract_replay": True,
            "live_contract": True,
        }
        rows.append(row)
    return rows


def _write_csv(path, rows, columns):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path, rows, columns):
    bool_columns = [column for column in columns if column != "stream"]

    def as_cell(column, value):
        if column in bool_columns:
            return "✅" if value else ""
        return str(value)

    with path.open("w", encoding="utf-8") as handle:
        handle.write("# Per-stream test matrix\n\n")
        handle.write("Generated from `tests/base.py` and integration test scope conventions.\n\n")
        handle.write("| " + " | ".join(columns) + " |\n")
        handle.write("| " + " | ".join(["---"] * len(columns)) + " |\n")
        for row in rows:
            handle.write(
                "| " + " | ".join(as_cell(column, row[column]) for column in columns) + " |\n"
            )


def main():
    repo_root = Path(__file__).resolve().parents[1]
    base_path = repo_root / "tests" / "base.py"
    output_dir = repo_root / "test_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    stream_metadata, pr_tier, nightly_tier, exclusions = _extract_metadata(base_path)
    rows = _rows(stream_metadata, pr_tier, nightly_tier, exclusions)

    columns = [
        "stream",
        "in_pr_tier",
        "in_nightly_tier",
        "discovery",
        "all_fields",
        "automatic_fields",
        "interrupted_sync",
        "pagination",
        "bookmark",
        "start_date",
        "auth_config_negative",
        "contract_replay",
        "live_contract",
    ]

    _write_csv(output_dir / "stream_test_matrix.csv", rows, columns)
    _write_markdown(output_dir / "stream_test_matrix.md", rows, columns)


if __name__ == "__main__":
    main()
