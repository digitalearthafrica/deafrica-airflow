import json
import importlib
from pathlib import Path

mod_spec = importlib.util.spec_from_file_location(
    "sentinel-2_data_transfer",
    Path(__file__).parent / "../../dags/sentinel-2_data_transfer.py",
)
s2_data_transfer_module = importlib.util.module_from_spec(mod_spec)
mod_spec.loader.exec_module(s2_data_transfer_module)


def test_correct_stac_links():
    with open(Path(__file__).parent / "S2B_37JHN_20210210_0_L2A.json") as f:
        source_doc = f.read()

    output_doc = s2_data_transfer_module.correct_stac_links(json.loads(source_doc))

    output_text = json.dumps(output_doc)

    assert "https://sentinel-cogs.s3.us-west-2.amazonaws.com" not in output_text

    # Ensure canonical and via-cirrus links have been dropped
    link_types = [link["rel"] for link in output_doc["links"]]
    assert "canonical" not in link_types
    assert "via-cirrus" not in link_types

    # Ensure we're pointing to the correct bucket in our links
    for link in output_doc["links"]:
        if link["rel"] == "self":
            assert link["href"].startswith("s3://deafrica-sentinel-2/")
        if link["rel"] == "derived_from":
            assert link["href"].startswith("s3://sentinel-cogs/")
