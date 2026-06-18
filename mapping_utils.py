def set_nested_value(data: dict, path: str, value):
    """Helper to set nested dictionary values using dot notation."""
    if not path or value is None: return
    parts = path.split('.')
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value

def map_dynamic_outbound(ekyam_order: dict, config: dict) -> dict:
    
    target_payload = {}
    mapping = config.get("mapping", {})
    
    # ( ext_order_id -> id)
    for target_field, ekyam_field in mapping.get("fields", {}).items():
        val = ekyam_order.get(ekyam_field)
        if val is not None:
            set_nested_value(target_payload, target_field, val)
    
     # (e.g., order_lines -> line_items)        
    for list_name, list_config in mapping.get("lists", {}).items():
        ekyam_list = ekyam_order.get(list_config.get("path"), [])
        target_list = []
        for item in ekyam_list:
            mapped_item = {}
            for target_item_field, ekyam_field in list_config.get("fields", {}).items():
                val = item.get(ekyam_field)
                if target_item_field == "product_id" and val == "custom_product":
                    continue
                if val is not None:
                    set_nested_value(mapped_item, target_item_field, val)
            target_list.append(mapped_item)
        if target_list:
            set_nested_value(target_payload, list_name, target_list)
            
    return target_payload

def prepare_shopify_order_for_push(ekyam_order: dict, mapping_config: dict) -> dict:
  
    shopify_order = map_dynamic_outbound(ekyam_order, mapping_config)
    
    shopify_order.pop("id", None)
    shopify_order.pop("shop_domain", None)
    
    # Shopify API requires a 'title' for line items if product_id isn't being used
    # to link to an existing product on the target store.
    if "line_items" in shopify_order:
        for item in shopify_order["line_items"]:
            if not item.get("title"):
                fallback_name = item.get("sku") or "Custom Product"
                item["title"] = fallback_name
                item["name"] = fallback_name
                
    return shopify_order