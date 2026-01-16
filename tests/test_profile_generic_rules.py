import pytest
import pandas as pd

from dq_suite.profile.generic_rules import create_dq_rules


# Dummy Geometry class
class Geometry:
    def __init__(self, wkb, srid):
        self.wkb = wkb
        self.srid = srid

    def __repr__(self):
        return f"Geometry({self.wkb!r}, {self.srid})"
    
    
@pytest.fixture
def df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "amount": [10, 20, 30],
        "status": ["open", "closed", "open"],
        "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "price": [1.5, 2.5, 99.9],
    })
    

@pytest.fixture
def df_with_geometry():
    return pd.DataFrame({
        "geometry": [
            Geometry(b"\x01\x06\x00\x00\x00\x01\x00\x00\x00", 4326),
            Geometry(b"\x01\x06\x00\x00\x00\x01\x00\x00\x00", 4326),
        ],
        "id": [1, 2],
    })


@pytest.fixture
def profiling_input():
    return {
        "table": {"n": 1000},
        "variables": {
            "id": {
                "type": "Numeric",
                "p_distinct": 1.0,
                "p_missing": 0.0,
                "min": 1,
                "max": 100,
            },
            "timestamp": {
                "type": "DateTime",
                "p_missing": 0.0,
            },
            "status": {
                "type": "Categorical",
                "n_distinct": 3,
                "value_counts_without_nan": {
                    "open": 500,
                    "closed": 400,
                    "pending": 100,
                },
            },
            "amount": {
                "type": "Numeric",
                "min": 1,
                "max": 100,
            },
            "price": {
                "type": "Numeric",
                "min": 1.5,
                "max": 99.9,
            },
        },
    }


def test_dq_structure(profiling_input, df):
    dataset_name = "test_dataset"
    table_name = "test_table"
    dq_json = create_dq_rules(dataset_name, table_name, profiling_input, df)

    assert dq_json["dataset"].name == dataset_name
    assert dq_json["tables"][0].table_name == table_name


def test_missing_table_key_raises_error(df):
    """Ensure KeyError is raised when table metadata is missing."""
    profiling_input = {"variables": {"id": {"type": "Integer"}}}
    with pytest.raises(KeyError):
        create_dq_rules("dataset", "table", profiling_input, df)


def test_missing_variables_key_raises_error(df):
    """Ensure KeyError is raised when variables metadata is missing."""
    profiling_input = {"table": {"n": 1000}}
    with pytest.raises(KeyError):
        create_dq_rules("dataset", "table", profiling_input, df)


def test_rule_logic(profiling_input, df):
    dq_json = create_dq_rules("test_dataset", "test_table", profiling_input, df)
    rules = dq_json["tables"][0].rules
    rule_names = [rule.rule_name for rule in rules]
    columns = list(profiling_input["variables"].keys())

    # Check key expected rules
    assert "ExpectColumnValuesToBeUnique" in rule_names
    assert "ExpectColumnValuesToNotBeNull" in rule_names
    assert "ExpectColumnValuesToBeBetween" in rule_names
    assert "ExpectTableRowCountToBeBetween" in rule_names
    assert "ExpectColumnValuesToBeInSet" in rule_names
    assert any(
        rule.rule_name == "ExpectTableColumnsToMatchSet"
        and rule.parameters["column_set"] == columns
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["column"] == "id"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToMatchRegex"
        and rule.parameters["column"] == "timestamp"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "IntegerType"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "DoubleType"
        for rule in rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfType"
        and rule.parameters["type_"] == "StringType"
        for rule in rules
    )


def test_geometry_rules_are_created(df_with_geometry):
    profiling_input = {
        "table": {"n": 10},
        "variables": {
            "geometry": {
                "type": "Unsupported",
                "p_missing": 0.0,
            }
        },
    }

    dq_json = create_dq_rules(
        dataset_name="geo_dataset",
        table_name="geo_table",
        profiling_json=profiling_input,
        df=df_with_geometry,
    )

    rules = dq_json["tables"][0].rules
    rule_names = [rule.rule_name for rule in rules]

    assert "ExpectGeometryColumnValuesToNotBeEmpty" in rule_names
    assert "ExpectColumnValuesToBeOfGeometryType" in rule_names
    assert "ExpectColumnValuesToHaveValidGeometry" in rule_names

    # Check parameters for geometry rules
    geometry_rules = [
        rule for rule in rules if rule.parameters.get("column") == "geometry"
    ]

    assert any(
        rule.rule_name == "ExpectGeometryColumnValuesToNotBeEmpty"
        and rule.rule_type == "geo"
        for rule in geometry_rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToBeOfGeometryType"
        and rule.parameters.get("geometry_type") == "GEOMETRY TYPE TO BE FILLED IN"
        for rule in geometry_rules
    )
    assert any(
        rule.rule_name == "ExpectColumnValuesToHaveValidGeometry"
        for rule in geometry_rules
    )