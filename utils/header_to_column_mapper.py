def rename_columns(header_mapping):
    def rename_dict_keys(d):
        result = {}
        for k, v in d.items():
            new_key = header_mapping.get(k, k)  # Use mapped name or keep original
            result[new_key] = v
        return result
    return rename_dict_keys