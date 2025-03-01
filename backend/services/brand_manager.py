import json

# Load predefined brand list
with open("data/predefined_brands.json", "r") as f:
    brand_data = json.load(f)

def get_brand_category(mcat_name, brand_name):
    """Check if the brand exists in predefined categories."""
    if mcat_name in brand_data:
        brands = brand_data[mcat_name]
        return brands.get(brand_name)  # Returns Premium, Mid-range, or Generic
    return None
