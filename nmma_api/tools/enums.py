import requests
import yaml
import os

# we map sncosmo filters for which we have no trained models to similar filters for which we do have trained models

REPO = "https://gitlab.com/Theodlz/nmma-models/raw/main/models.yaml"

FILTERS_MAPPER = {
    "sdssg": "ps1__g",
    "sdssi": "ps1__i",
    "sdssr": "ps1__r",
    "sdssz": "ps1__z",
    "sdssu": "ps1__u",
}


def fetch_models():
    # check if the file exists
    try:
        if os.path.exists("models.yaml"):
            with open("models.yaml", "r") as f:
                return yaml.safe_load(f)
    except Exception:
        pass

    response = requests.get(REPO)
    content = response.content.decode("utf-8")
    models = yaml.safe_load(content)
    # save to file
    with open("models.yaml", "w") as f:
        yaml.dump(models, f)

    return models


CENTRAL_WAVELENGTH_MODELS = ["Me2017", "Piro2021", "nugent-hyper", "TrPi2018"]
FIXED_FILTERS_MODELS = fetch_models()


def verify_and_match_filter(model, filter):
    if model in CENTRAL_WAVELENGTH_MODELS:
        return filter

    # we only support _tf models for now, so if the model does not end with _tf, we add it
    if not model.endswith("_tf"):
        model = model + "_tf"

    if model not in FIXED_FILTERS_MODELS:
        raise ValueError(f"Model {model} not found")

    if filter not in FIXED_FILTERS_MODELS[model].get("filters", []):
        # see if there is a similar filter
        replacement = FILTERS_MAPPER.get(filter)
        if replacement and replacement in FIXED_FILTERS_MODELS[model].get(
            "filters", []
        ):
            return replacement
        raise ValueError(f"Filter {filter} not found in model {model}")

    return filter
