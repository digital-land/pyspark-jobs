
#Fact table busines logic
# 1. Field Normalisation: fields_str = ", ".join([field.replace("-", "_") for field in fact_fields])
# 2. Deduplication Logic: Rule: For each fact, keep only the highest priority or most recent record. Purpose: Ensures only the most relevant version of each fact is loaded.
