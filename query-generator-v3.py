import json
import re

def load_reapi_documentation(file_path='reapi_documentation.json'):
    with open(file_path, 'r') as f:
        return json.load(f)

def generate_reapi_query(description, doc):
    query = {}
    
    # Check for count request
    if "how many" in description.lower() or "count" in description.lower():
        query["count"] = True
    else:
        query["size"] = 100  # Default size if not specified
    
    # Check for property types
    property_types = [pt for pt in doc["property_types"] if pt.lower() in description.lower()]
    if property_types:
        query["property_type"] = property_types if len(property_types) > 1 else property_types[0]
    
    # Check for boolean filters
    for filter in doc["boolean_filters"]:
        if filter.replace("_", " ") in description.lower():
            query[filter] = True
    
    # Check for range filters
    for range_filter in doc["range_filters"]:
        pattern = rf'{range_filter["name"].replace("_", " ")} between (\d+)k? and (\d+)k?'
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            min_value = int(match.group(1)) * 1000 if 'k' in match.group(1).lower() else int(match.group(1))
            max_value = int(match.group(2)) * 1000 if 'k' in match.group(2).lower() else int(match.group(2))
            query[range_filter["min"]] = min_value
            query[range_filter["max"]] = max_value
    
    # Check for operator filters
    for op_filter in doc["operator_filters"]:
        pattern = rf'{op_filter["name"].replace("_", " ")} (less than|greater than) (\d+)k?'
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            value = int(match.group(2)) * 1000 if 'k' in match.group(2).lower() else int(match.group(2))
            query[op_filter["name"]] = value
            query[op_filter["operator"]] = "lt" if match.group(1) == "less than" else "gt"
    
    # Check for geo filters
    for geo_filter in doc["geo_filters"]:
        if geo_filter in ["latitude", "longitude", "radius"]:
            pattern = rf'{geo_filter} (\d+\.?\d*)'
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                query[geo_filter] = float(match.group(1))
        else:
            pattern = rf'in (\w+) {geo_filter}'
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                query[geo_filter] = match.group(1)
    
    # Check for special filters
    for special_filter in doc["special_filters"]:
        if special_filter in description.lower():
            query[special_filter] = True
    
    # Check for enumeration fields
    for enum_field in doc["enumeration_fields"]:
        pattern = rf'{enum_field.replace("_", " ")} (\w+)'
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            query[enum_field] = match.group(1)
    
    # Check for autocomplete fields
    for auto_field in doc["autocomplete_fields"]:
        pattern = rf'{auto_field.replace("_", " ")} (\w+)'
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            query[auto_field] = match.group(1)
    
    # Check for sort fields
    sort_pattern = r'sort by (\w+) (ascending|descending)'
    sort_match = re.search(sort_pattern, description, re.IGNORECASE)
    if sort_match and sort_match.group(1) in doc["sort_fields"]:
        query["sort"] = {sort_match.group(1): "asc" if sort_match.group(2) == "ascending" else "desc"}
    
    return query

# Load the documentation
reapi_doc = load_reapi_documentation()

# Example usage
description = "Find pre-foreclosure single family homes and condos in Orange county, Florida with value between 200k and 500k, built after 1990, and sort by estimated equity descending"
query = generate_reapi_query(description, reapi_doc)
print(json.dumps(query, indent=2))
