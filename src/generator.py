import argparse
import os
import xmlschema
from typing import Dict, List, Optional, Union
import yaml
from pathlib import Path
import json
import logging
from collections import defaultdict
from xmlschema.validators.identities import XsdKey, XsdUnique, XsdKeyref
from xmlschema.validators.groups import XsdGroup
from xmlschema.validators.elements import XsdElement

# Fix import for both module and direct script usage
try:
    from src.xsdtojson import xsd_to_json_schema
except ImportError:
    from xsdtojson import xsd_to_json_schema
from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.dumpers import yaml_dumper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _load_doc_overrides(path: Optional[Union[str, Path]] = None) -> Dict:
    """
    Load documentation override configuration from YAML.
    Structure:
    attribute_descriptions:
      ClassName:
        AttributeName: "Override description"
    """
    candidate_paths: List[Path] = []
    if path:
        candidate_paths.append(Path(path))
    else:
        default_path = Path(__file__).resolve().parent / "config" / "doc_overrides.yaml"
        candidate_paths.append(default_path)
    for candidate in candidate_paths:
        try:
            if candidate and candidate.exists():
                with candidate.open("r") as fh:
                    data = yaml.safe_load(fh) or {}
                    logger.debug(f"Loaded documentation overrides from {candidate}")
                    return data
        except Exception as exc:
            logger.warning(f"Failed to load documentation overrides from {candidate}: {exc}")
    return {}

def generate_linkml_schema(ome_xsd_path, output_path=None, top_level_elements=None, partition=False,
                           schema_id=None, schema_name=None, schema_title=None, default_prefix=None,
                           extra_prefixes=None, json_output_path=None, doc_overrides_path=None):
    """
    Generate a LinkML schema from an OME XSD file.
    
    Args:
        ome_xsd_path: Path to the OME XSD file
        output_path: Path to output the LinkML schema
        top_level_elements: List of top-level elements to include (if None, include all)
        partition: Whether to partition the schema into separate files
    
    Returns:
        A dictionary containing the LinkML schema
    """
    try:
        # Parse the XSD using xmlschema
        xsd = xmlschema.XMLSchema(ome_xsd_path)
        
        # Convert to JSON Schema
        json_schema = xsd_to_json_schema(ome_xsd_path)
        
        # Filter top-level elements if specified
        if top_level_elements:
            filtered_props = {}
            filtered_defs = {}
            
            # Keep only specified top-level elements
            for element in top_level_elements:
                if element in json_schema.get("properties", {}):
                    filtered_props[element] = json_schema["properties"][element]
                
                # Include associated definitions
                if "definitions" in json_schema:
                    for def_name, def_value in json_schema["definitions"].items():
                        if def_name.startswith(element) or def_name in [ref.split("/")[-1] for ref in filtered_props.get(element, {}).get("$ref", "").split()]:
                            filtered_defs[def_name] = def_value
            
            # Update JSON schema with filtered values
            json_schema["properties"] = filtered_props
            if filtered_defs:
                json_schema["definitions"] = filtered_defs
        
        # Optionally write intermediate JSON Schema
        if json_output_path:
            try:
                out_dir = os.path.dirname(json_output_path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with open(json_output_path, 'w') as jf:
                    json.dump(json_schema, jf, indent=2)
            except Exception as e:
                logger.warning(f"Failed writing JSON Schema to {json_output_path}: {e}")

        # Convert JSON Schema to LinkML
        metadata = {
            "schema_id": schema_id,
            "schema_name": schema_name,
            "schema_title": schema_title,
            "default_prefix": default_prefix,
            "extra_prefixes": extra_prefixes or {}
        }
        linkml_schema = convert_json_schema_to_linkml(
            json_schema,
            xsd,
            metadata,
            overrides_path=doc_overrides_path
        )
        
        # Output schema
        if output_path:
            if partition and "classes" in linkml_schema:
                # Create directory if it doesn't exist
                if not os.path.exists(output_path):
                    os.makedirs(output_path)
                
                # Partition schema by top-level classes (generic)
                for class_name, class_def in list(linkml_schema["classes"].items()):
                    partitioned_schema = {
                        "id": linkml_schema["id"],
                        "name": linkml_schema["name"],
                        "title": linkml_schema["title"],
                        "description": linkml_schema["description"],
                        "license": linkml_schema["license"],
                        "version": linkml_schema["version"],
                        "prefixes": linkml_schema["prefixes"],
                        "default_prefix": linkml_schema["default_prefix"],
                        "types": linkml_schema["types"],
                        "classes": { class_name: class_def },
                        "slots": {}
                    }

                    # Include slots referenced by this class
                    class_slots = set(class_def.get("slots", []) or [])
                    slot_registry = linkml_schema.get("slots", {})
                    for slot_name in class_slots:
                        if slot_name in slot_registry:
                            partitioned_schema["slots"][slot_name] = slot_registry[slot_name]

                    # Naive inclusion of classes referenced by slot ranges
                    referenced_classes = set()
                    for slot_name in class_slots:
                        slot_def = slot_registry.get(slot_name, {})
                        rng = slot_def.get("range")
                        if rng and rng in linkml_schema["classes"] and rng not in partitioned_schema["classes"]:
                            referenced_classes.add(rng)
                    for ref_cls in referenced_classes:
                        partitioned_schema["classes"][ref_cls] = linkml_schema["classes"][ref_cls]

                    class_file_path = os.path.join(output_path, f"{class_name}.yaml")
                    with open(class_file_path, 'w') as f:
                        yaml.dump(partitioned_schema, f, sort_keys=False)
                
                logger.info(f"Successfully partitioned schema into {len(linkml_schema['classes'])} files in {output_path}")
            else:
                # Write full schema to a single file
                # Ensure the output path has a .yaml extension
                if not output_path.endswith('.yaml') and not output_path.endswith('.yml'):
                    output_path = f"{output_path}.yaml"
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
                
                # Use yaml_dumper for consistent YAML format
                with open(output_path, 'w') as f:
                    yaml.dump(linkml_schema, f, sort_keys=False, default_flow_style=False)
                
                logger.info(f"Successfully generated LinkML schema at {output_path}")
        
        return linkml_schema
    
    except Exception as e:
        logger.error(f"Error generating LinkML schema: {str(e)}")
        raise

def convert_json_schema_to_linkml(json_schema, xsd, metadata=None, overrides_path=None):
    """
    Convert a JSON Schema to a LinkML schema.
    
    Args:
        json_schema: JSON Schema dictionary
        xsd: The original XMLSchema object for documentation and inheritance information
    
    Returns:
        A dictionary containing the LinkML schema
    """
    # Derive schema metadata from XSD namespace if not provided
    target_ns = getattr(xsd, 'target_namespace', None)
    ns_suffix = None
    if target_ns:
        try:
            ns_clean = str(target_ns).rstrip('/')
            ns_suffix = ns_clean.split('/')[-1] or None
        except Exception:
            ns_suffix = None
    inferred_prefix = None
    if ns_suffix:
        inferred_prefix = ns_suffix.lower().replace('-', '_').replace(' ', '_')
    # Fallback heuristics
    if not inferred_prefix and target_ns:
        if 'openmicroscopy' in target_ns:
            inferred_prefix = 'ome'
        elif 'bina' in target_ns or 'microscopy' in target_ns:
            inferred_prefix = 'nbo'
    if not inferred_prefix:
        inferred_prefix = 'schema'

    # Ensure valid NCName for LinkML name/prefix (must not start with a digit)
    try:
        import re as _re
        if not _re.match(r'^[A-Za-z_]', inferred_prefix):
            inferred_prefix = f"ns_{inferred_prefix}"
    except Exception:
        if not (inferred_prefix[:1].isalpha() or inferred_prefix[:1] == '_'):
            inferred_prefix = f"ns_{inferred_prefix}"

    meta = metadata or {}
    schema_id_val = meta.get("schema_id") or (f"{target_ns.rstrip('/')}/linkml" if target_ns else "https://w3id.org/linkml/schema")
    schema_name_val = meta.get("schema_name") or inferred_prefix
    schema_title_val = meta.get("schema_title") or f"{schema_name_val.upper()} Schema"
    default_prefix_val = meta.get("default_prefix") or schema_name_val

    prefixes_val = {
        "linkml": "https://w3id.org/linkml/",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        schema_name_val: (str(target_ns) if target_ns else f"https://example.org/{schema_name_val}#"),
        "schema": "http://schema.org/"
    }
    # Merge any extra prefixes
    for k, v in (meta.get("extra_prefixes") or {}).items():
        prefixes_val[k] = v

    # Create basic LinkML schema structure
    doc_overrides = _load_doc_overrides(overrides_path)
    attr_description_overrides = doc_overrides.get("attribute_descriptions", {})

    linkml_schema = {
        "id": schema_id_val,
        "name": schema_name_val,
        "title": schema_title_val,
        "description": "LinkML translation of the provided XML Schema",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "version": "0.0.1",
        "prefixes": prefixes_val,
        "default_prefix": default_prefix_val,
        "default_range": "string",
        "subsets": {
            "NBO_Tier1": {"description": "NBO Tier 1 (minimal) metadata"},
            "NBO_Tier2": {"description": "NBO Tier 2 (recommended) metadata"},
            "NBO_Tier3": {"description": "NBO Tier 3 (advanced) metadata"}
        },
        # omit custom types; rely on LinkML builtins
        "classes": {},
        "slots": {},
        "enums": {}
    }

    def _local_name(value: Optional[Union[str, object]]) -> str:
        if value is None:
            return ""
        text = str(value)
        if "}" in text:
            return text.split("}")[-1]
        if ":" in text:
            return text.split(":")[-1]
        return text

    primitive_ranges = {
        "string",
        "integer",
        "float",
        "boolean",
        "date",
        "time",
        "datetime",
        "uri",
    }

    choice_slot_membership: Dict[str, set] = defaultdict(set)
    choice_repeat_membership: Dict[str, set] = defaultdict(set)

    known_class_names = set()
    for elem_name in getattr(xsd, "elements", {}):
        local = _local_name(elem_name)
        if local:
            known_class_names.add(local)
    for type_name, type_def in getattr(xsd, "types", {}).items():
        if isinstance(type_name, str):
            local = _local_name(type_name)
        else:
            local = _local_name(getattr(type_def, "name", None))
        if local:
            known_class_names.add(local)

    def _map_xsd_primitive(xsd_type) -> str:
        """Best-effort mapping of an XSD simple type to a LinkML primitive."""
        if xsd_type is None:
            return "string"
        type_name = None
        if hasattr(xsd_type, "name") and xsd_type.name:
            type_name = _local_name(xsd_type.name)
        elif isinstance(xsd_type, str):
            type_name = _local_name(xsd_type)
        elif hasattr(xsd_type, "base_type") and xsd_type.base_type is not None:
            return _map_xsd_primitive(xsd_type.base_type)
        if not type_name:
            return "string"
        mapping = {
            "string": "string",
            "token": "string",
            "normalizedString": "string",
            "anyURI": "uri",
            "float": "float",
            "double": "float",
            "decimal": "float",
            "integer": "integer",
            "int": "integer",
            "long": "integer",
            "short": "integer",
            "byte": "integer",
            "nonNegativeInteger": "integer",
            "positiveInteger": "integer",
            "unsignedLong": "integer",
            "unsignedInt": "integer",
            "unsignedShort": "integer",
            "unsignedByte": "integer",
            "boolean": "boolean",
            "date": "date",
            "dateTime": "datetime",
            "time": "time",
            "ID": "string",
            "IDREF": "string",
            "IDREFS": "string",
        }
        if type_name in mapping:
            return mapping[type_name]
        base_type = getattr(xsd_type, "base_type", None)
        if base_type is not None and base_type is not xsd_type:
            mapped = _map_xsd_primitive(base_type)
            if mapped:
                return mapped
        return "string"

    def _ensure_class(name: str, default_description: Optional[str] = None) -> Dict:
        cls = linkml_schema["classes"].setdefault(name, {})
        if default_description:
            if not cls.get("description"):
                cls["description"] = default_description
        cls.setdefault("attributes", {})
        return cls

    def _merge_slot(existing: Dict, incoming: Dict) -> Dict:
        merged = existing or {}
        for key, value in incoming.items():
            if value is None:
                continue
            if key not in merged:
                merged[key] = value
            elif key == "description" and not merged.get(key):
                merged[key] = value
            elif key == "range":
                new_range = value
                if not new_range:
                    continue
                existing_range = merged.get("range")
                if new_range == "string" and existing_range and existing_range != "string":
                    continue
                if (not existing_range) or existing_range == "string" or new_range != "string":
                    merged["range"] = new_range
            elif key == "in_subset":
                current = merged.setdefault("in_subset", [])
                for item in value:
                    if item not in current:
                        current.append(item)
            elif key == "annotations":
                ann_target = merged.setdefault("annotations", {})
                for ann_key, ann_val in value.items():
                    if ann_key not in ann_target:
                        ann_target[ann_key] = ann_val
        return merged

    def _add_attribute(class_name: str, slot_name: str, slot_definition: Dict):
        cls = _ensure_class(class_name)
        attrs = cls.setdefault("attributes", {})
        if slot_name in attrs:
            attrs[slot_name] = _merge_slot(attrs[slot_name], slot_definition)
        else:
            attrs[slot_name] = slot_definition

    def _add_unique_key(class_name: str, key_name: str, slot_names: List[str]):
        if not slot_names:
            return
        cls = _ensure_class(class_name)
        unique_keys = cls.setdefault("unique_keys", {})
        entry = unique_keys.setdefault(key_name, {"unique_key_slots": []})
        for slot in slot_names:
            if slot not in entry["unique_key_slots"]:
                entry["unique_key_slots"].append(slot)

    tier_subset_lookup = {
        "1": "NBO_Tier1",
        "2": "NBO_Tier2",
        "3": "NBO_Tier3"
    }

    def _split_doc_metadata(doc_text: str):
        metadata: Dict[str, List[str]] = defaultdict(list)
        description_lines: List[str] = []
        for raw_line in doc_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    description_lines.append(line)
                    continue
                if key.lower() == "description":
                    description_lines.append(value)
                else:
                    metadata[key].append(value)
            else:
                description_lines.append(line)
        description = "\n".join(description_lines).strip()
        return description, metadata

    def _apply_metadata_to_target(target: Dict, metadata: Dict[str, List[str]]):
        if not metadata:
            return
        tier_values: List[str] = []
        for key in list(metadata.keys()):
            if key.lower() == "tier":
                tier_values.extend(metadata.pop(key) or [])
        if tier_values:
            subset_list = target.setdefault("in_subset", [])
            for raw_value in tier_values:
                if not raw_value:
                    continue
                for token in raw_value.replace(",", " ").split():
                    subset_name = tier_subset_lookup.get(token.strip())
                    if subset_name and subset_name not in subset_list:
                        subset_list.append(subset_name)
        if not metadata:
            return
        annotations = target.setdefault("annotations", {})
        for key, values in metadata.items():
            if not values:
                continue
            ann_base = key.replace(" ", "_")
            for idx, value in enumerate(values):
                ann_key = ann_base if idx == 0 and ann_base not in annotations else f"{ann_base}_{idx}"
                annotations[ann_key] = {
                    "tag": key,
                    "value": value
                }

    def _apply_appinfo_metadata(target: Dict, annotation):
        if annotation is None:
            return
        elem = getattr(annotation, "elem", None)
        if elem is None:
            return
        try:
            for appinfo in elem.findall('.//{*}appinfo'):
                for node in list(appinfo):
                    if _local_name(node.tag).lower() == "xsdfu":
                        for child in list(node):
                            _record_appinfo_entry(target, child)
                    else:
                        _record_appinfo_entry(target, node)
        except Exception:
            pass

    def _record_appinfo_entry(target: Dict, node):
        if node is None:
            return
        name = _local_name(node.tag)
        value = (node.text or "").strip() or "true"
        annotations = target.setdefault("annotations", {})
        key = name if name not in annotations else f"{name}_{len(annotations)}"
        annotations[key] = {
            "tag": name,
            "value": value
        }

    def _apply_doc_metadata(target: Dict, doc_value):
        if not doc_value:
            return
        clean_text = _coerce_description(doc_value)
        if not clean_text:
            return
        description, metadata = _split_doc_metadata(clean_text)
        if description:
            target["description"] = description
        _apply_metadata_to_target(target, metadata)

    def _class_is_ref_like(class_name: Optional[str]) -> bool:
        if not class_name:
            return False
        visited: set = set()
        current = class_name
        while current:
            if current.endswith("Ref") or current == "Reference":
                return True
            if current in visited:
                break
            visited.add(current)
            cls_def = linkml_schema["classes"].get(current)
            base = None
            if cls_def:
                base = cls_def.get("is_a")
            if not base:
                base = inheritance_map.get(current)
            current = base
        return False

    def _reference_target_for_class(owner_class: str, type_name_local: Optional[str]) -> Optional[str]:
        if owner_class and owner_class.endswith("Ref"):
            candidate = owner_class[:-3]
            if candidate in known_class_names:
                return candidate
        visited: set = set()
        current = owner_class
        while current:
            if current in visited:
                break
            visited.add(current)
            cls_def = linkml_schema["classes"].get(current)
            base = None
            if cls_def:
                base = cls_def.get("is_a")
                if base and base.endswith("Ref"):
                    candidate = base[:-3]
                    if candidate in known_class_names:
                        return candidate
            if not base:
                base = inheritance_map.get(current)
            current = base
        if type_name_local and type_name_local.endswith("ID"):
            candidate = type_name_local[:-2]
            if candidate in known_class_names:
                return candidate
        return None

    def _select_keyref_range(target_classes: set) -> Optional[str]:
        if not target_classes:
            return None
        if len(target_classes) == 1:
            return next(iter(target_classes))
        ancestor_lists: List[List[str]] = []
        for cls_name in target_classes:
            if not cls_name:
                continue
            ancestors: List[str] = []
            current = cls_name
            visited: set = set()
            while current and current not in visited:
                ancestors.append(current)
                visited.add(current)
                cls_def = linkml_schema["classes"].get(current)
                base = None
                if cls_def:
                    base = cls_def.get("is_a")
                if not base:
                    base = inheritance_map.get(current)
                current = base
            if ancestors:
                ancestor_lists.append(ancestors)
        if not ancestor_lists:
            return None
        common = set(ancestor_lists[0])
        for ancestors in ancestor_lists[1:]:
            common.intersection_update(ancestors)
            if not common:
                break
        if not common:
            return None
        for ancestor in ancestor_lists[0]:
            if ancestor in common:
                return ancestor
        return None

    def _ensure_enum_for_slot(class_name: str, attr_name: str, values: list) -> str:
        import re as _re
        base = f"Enum_{class_name}_{attr_name}"
        enum_name = _re.sub(r"[^A-Za-z0-9_]+", "_", base)
        if enum_name not in linkml_schema["enums"]:
            linkml_schema["enums"][enum_name] = {
                "permissible_values": {str(v): {} for v in values}
            }
        return enum_name

    def _slot_from_attribute(owner_class: str, attr_name: str, attr_obj) -> Dict:
        slot = {
            "range": "string"
        }
        attr_type = getattr(attr_obj, "type", None)
        slot["range"] = _map_xsd_primitive(attr_type)
        type_name_local = _local_name(getattr(attr_type, "name", None))
        attr_name_lower = attr_name.lower()
        if attr_name_lower == "id" and _class_is_ref_like(owner_class):
            target_candidate = _reference_target_for_class(owner_class, type_name_local)
            if target_candidate:
                slot["range"] = target_candidate
        doc = _get_documentation(getattr(attr_obj, "annotation", None))
        custom_doc = attr_description_overrides.get(owner_class, {}).get(attr_name)
        _apply_doc_metadata(slot, doc)
        if custom_doc:
            slot["description"] = custom_doc
        _apply_appinfo_metadata(slot, getattr(attr_obj, "annotation", None))
        enum_values = []
        if attr_type is not None:
            enum_values = [
                str(v) for v in getattr(attr_type, "enumeration", []) or []
            ]
        if enum_values:
            slot["range"] = _ensure_enum_for_slot(owner_class, attr_name, enum_values)
        facets = getattr(attr_type, "facets", {}) if attr_type is not None else {}
        pattern_facet = facets.get("{http://www.w3.org/2001/XMLSchema}pattern")
        if pattern_facet is not None:
            pattern_value = getattr(pattern_facet, "pattern", None)
            if not pattern_value:
                facet_patterns = getattr(pattern_facet, "patterns", None)
                if facet_patterns:
                    pattern_value = getattr(facet_patterns[0], "pattern", None)
            if not pattern_value:
                facet_regexps = getattr(pattern_facet, "regexps", None)
                if facet_regexps:
                    pattern_value = getattr(facet_regexps[0], "pattern", None)
            if pattern_value:
                slot["pattern"] = pattern_value
        min_inclusive = getattr(attr_type, "min_value", None)
        if min_inclusive is not None:
            slot["minimum_value"] = min_inclusive
        max_inclusive = getattr(attr_type, "max_value", None)
        if max_inclusive is not None:
            slot["maximum_value"] = max_inclusive
        if getattr(attr_obj, "use", None) == "required":
            slot["required"] = True
        # identifier heuristic
        if attr_name_lower == "id" or (type_name_local and type_name_local.endswith("ID")):
            slot["identifier"] = True
        return slot

    def _populate_complex_type(class_name: str, type_def, fallback_description: Optional[str] = None):
        if type_def is None:
            return
        cls = _ensure_class(class_name, fallback_description or f"Complex type {class_name}")
        if hasattr(type_def, "annotation") and type_def.annotation:
            doc = _get_documentation(type_def.annotation)
            _apply_doc_metadata(cls, doc)
            _apply_appinfo_metadata(cls, type_def.annotation)
        # Attributes
        for attr_qname, attr in getattr(type_def, "attributes", {}).items():
            attr_local = _local_name(attr_qname)
            slot = _slot_from_attribute(class_name, attr_local, attr)
            _add_attribute(class_name, attr_local, slot)
        # Child elements
        content = getattr(type_def, "content", None)
        if content is not None:
            if hasattr(content, "elements"):
                iterator = content.elements.items()
            elif hasattr(content, "iter_elements"):
                iterator = ((getattr(child, "name", None), child) for child in content.iter_elements())
            else:
                iterator = []
            for child_qname, child in iterator:
                _add_child_element(class_name, child_qname, child)
            _collect_choice_constraints(class_name, content)

    def _add_child_element(parent_class: str, child_qname, child):
        raw_name = child_qname or getattr(child, "name", None)
        child_name = _local_name(raw_name)
        if not child_name:
            child_name = _local_name(getattr(child, "name", None))
        if not child_name:
            return
        child_doc = _get_documentation(getattr(child, "annotation", None))
        slot_def: Dict[str, Union[str, bool]] = {}
        child_range = None
        ref_target = getattr(child, "ref", None)
        if ref_target is not None:
            ref_name = getattr(ref_target, "name", None)
            child_range = _local_name(ref_name if ref_name else ref_target)
        else:
            child_type = getattr(child, "type", None)
            if child_type is not None and getattr(child_type, "is_complex", lambda: False)():
                if getattr(child_type, "name", None):
                    child_range = _local_name(child_type.name)
                else:
                    child_range = child_name
                    _populate_complex_type(child_name, child_type, fallback_description=f"Inline complex type for {child_name}")
            elif child_type is not None:
                child_range = _map_xsd_primitive(child_type)
        if child_range and child_range not in primitive_ranges:
            _ensure_class(child_range)
        slot_def["range"] = child_range if child_range else "string"
        mino = getattr(child, "min_occurs", None)
        maxo = getattr(child, "max_occurs", None)
        occurs = getattr(child, "occurs", None)
        occurs_tuple = None
        if isinstance(occurs, tuple):
            occurs_tuple = occurs
        elif hasattr(occurs, "__iter__"):
            occurs_tuple = tuple(occurs)
        if maxo is None and occurs_tuple:
            try:
                maxo = occurs_tuple[-1]
            except IndexError:
                maxo = None
        if isinstance(mino, int) and mino >= 1:
            slot_def["required"] = True
        if maxo == "unbounded" or (maxo is None and occurs_tuple and occurs_tuple[-1] is None) or (isinstance(maxo, int) and maxo > 1):
            slot_def["multivalued"] = True
        if child_doc:
            _apply_doc_metadata(slot_def, child_doc)
        else:
            slot_def.setdefault("description", f"Child element {child_name} of {parent_class}")
        _apply_appinfo_metadata(slot_def, getattr(child, "annotation", None))
        _add_attribute(parent_class, child_name, slot_def)

    def _choice_is_repeating(group: XsdGroup) -> bool:
        maxo = getattr(group, "max_occurs", None)
        occurs = getattr(group, "occurs", None)
        occurs_tuple = None
        if isinstance(occurs, tuple):
            occurs_tuple = occurs
        elif hasattr(occurs, "__iter__"):
            occurs_tuple = tuple(occurs)
        if maxo == "unbounded" or (isinstance(maxo, int) and maxo > 1):
            return True
        if maxo is None and occurs_tuple and occurs_tuple[-1] is None:
            return True
        return False

    def _collect_choice_constraints(class_name: str, content):
        if content is None:
            return
        if not hasattr(content, "iter_model"):
            return
        if isinstance(content, XsdGroup) and content.model == 'choice':
            branches = _extract_choice_branches(content)
            _apply_choice_constraint(class_name, branches, _choice_is_repeating(content))
        for item in content.iter_model():
            if isinstance(item, XsdGroup) and item.model == 'choice':
                branches = _extract_choice_branches(item)
                _apply_choice_constraint(class_name, branches, _choice_is_repeating(item))

    def _extract_choice_branches(group: XsdGroup) -> List[List[tuple]]:
        branches: List[List[tuple]] = []
        for item in group.iter_model():
            if isinstance(item, XsdGroup):
                if item.model == 'choice':
                    branches.extend(_extract_choice_branches(item))
                else:
                    branches.append(_collect_branch_slots(item))
            elif isinstance(item, XsdElement):
                branches.append(_collect_branch_slots(item))
        return [branch for branch in branches if branch]

    def _collect_branch_slots(node) -> List[tuple]:
        slots: List[tuple] = []

        def _visit(item):
            if isinstance(item, XsdElement):
                slot_name = _local_name(getattr(item, "name", None) or getattr(getattr(item, "ref", None), "name", None))
                if slot_name:
                    required = bool(getattr(item, "min_occurs", 0))
                    slots.append((slot_name, required))
            elif isinstance(item, XsdGroup):
                for sub in item.iter_model():
                    _visit(sub)

        _visit(node)
        return slots

    def _apply_choice_constraint(class_name: str, branches: List[List[tuple]], repeating: bool = False):
        if len(branches) < 2:
            return
        cls = _ensure_class(class_name)
        slot_sets = [set(slot for slot, _ in branch) for branch in branches]
        universe = set().union(*slot_sets)
        if all(len(slot_set) == 1 for slot_set in slot_sets):
            group: List[str] = []
            for slot_set in slot_sets:
                slot_name = next(iter(slot_set))
                if slot_name not in group:
                    group.append(slot_name)
            existing = cls.setdefault("exactly_one_of", [])
            for slot_name in group:
                expr = {
                    "slot_conditions": {
                        slot_name: {"required": True}
                    }
                }
                if expr not in existing:
                    existing.append(expr)
            choice_slot_membership[class_name].update(universe)
            if repeating:
                choice_repeat_membership[class_name].update(universe)
            return
        # For complex choice branches (where a branch may contain multiple slots),
        # we skip emitting a formal LinkML constraint because the current LinkML
        # schema requires slot-level expressions rather than the shorthand we use
        # for single-slot branches. We still relax the per-slot required/multivalued
        # flags via choice_slot_membership so that instances can include any valid
        # branch without triggering validation errors.
        choice_slot_membership[class_name].update(universe)
        if repeating:
            choice_repeat_membership[class_name].update(universe)

    def _apply_local_identities(class_name: str, elem_def):
        local_ids = getattr(elem_def, "identities", None)
        if not local_ids:
            return
        cls = _ensure_class(class_name)
        annotations = cls.setdefault("annotations", {})
        for ident in local_ids:
            try:
                ident_name = _local_name(getattr(ident, "name", "") or "local_identity")
                selector_path = getattr(getattr(ident, "selector", None), "path", "") or ""
                field_paths = [getattr(field, "path", "") for field in getattr(ident, "fields", []) or []]
                annotations[f"local_identity_{ident_name}"] = {
                    "tag": "local_identity",
                    "value": json.dumps({
                        "type": ident.__class__.__name__,
                        "selector": selector_path,
                        "fields": field_paths
                    })
                }
            except Exception:
                continue

    def _extract_text(obj):
        # Element nodes
        if hasattr(obj, 'tag') and hasattr(obj, 'text'):
            return (obj.text or "").strip()
        # Strings
        if isinstance(obj, str):
            return obj.strip()
        # Dicts
        if isinstance(obj, dict):
            import json as _json
            return _json.dumps(obj, ensure_ascii=False)
        # Lists/tuples
        if isinstance(obj, (list, tuple)):
            parts = []
            for item in obj:
                parts.append(_extract_text(item))
            return "\n".join([p for p in parts if p])
        # Fallback
        return str(obj)

    def _coerce_description(value):
        try:
            if value is None:
                return ""
            return _extract_text(value)
        except Exception:
            return _extract_text(value)
    
    # Get complex type hierarchy from XSD
    complex_types = {}
    inheritance_map = {}
    
    # Extract complex types and their inheritance
    for type_name, type_def in xsd.types.items():
        if not type_def.is_complex():
            continue
        local_name = _local_name(type_name) if isinstance(type_name, str) else _local_name(getattr(type_def, "name", None))
        if not local_name:
            continue
        complex_types[local_name] = type_def
        # Check for base types (inheritance)
        if hasattr(type_def, 'content') and hasattr(type_def.content, 'base_type'):
            base_type = type_def.content.base_type
            if base_type and hasattr(base_type, 'name') and base_type.name:
                base_name = _local_name(base_type.name)
                if base_name:
                    inheritance_map[local_name] = base_name
    
    # Create a class for each complex type
    for type_name, type_def in complex_types.items():
        _populate_complex_type(type_name, type_def, fallback_description=f"Complex type {type_name}")
        cls = _ensure_class(type_name)
        # Add inheritance (is_a)
        if type_name in inheritance_map:
            base_type = inheritance_map[type_name]
            if base_type in linkml_schema["classes"]:
                cls["is_a"] = base_type
        
    # Process elements to create classes and slots
    for elem_name, elem_def in xsd.elements.items():
        element_name = str(elem_name).split("}")[-1]  # Remove namespace safely
        # Create or reuse class for element
        cls = _ensure_class(element_name, f"The {element_name} element from the XML Schema.")
        
        elem_type = getattr(elem_def, 'type', None)
        processed_inline_type = False
        if elem_type is not None and hasattr(elem_type, "is_complex") and elem_type.is_complex():
            if not getattr(elem_type, "name", None):
                _populate_complex_type(element_name, elem_type, fallback_description=cls.get("description"))
                processed_inline_type = True

        # Get documentation if available
        if hasattr(elem_def, 'annotation') and elem_def.annotation:
            doc = _get_documentation(elem_def.annotation)
            _apply_doc_metadata(cls, doc)
            _apply_appinfo_metadata(cls, elem_def.annotation)
        
        # Mark abstract if applicable
        try:
            if getattr(elem_def, 'abstract', False) or getattr(elem_def, 'is_abstract', False):
                cls["abstract"] = True
        except Exception:
            pass

        # Direct type reference: if element is declared with a named complex type
        try:
            if hasattr(elem_def, 'type') and hasattr(elem_def.type, 'name') and elem_def.type.name:
                direct_type_name = str(elem_def.type.name).split("}")[-1]
                if direct_type_name in linkml_schema["classes"] and direct_type_name != element_name:
                    cls["is_a"] = direct_type_name
        except Exception:
            pass

        # Substitution group: model as is_a to the head element class and mark head abstract
        try:
            sg = getattr(elem_def, 'substitution_group', None)
            if sg:
                head_name = str(sg).split("}")[-1]
                head_cls = _ensure_class(head_name, f"Head of substitution group {head_name}")
                head_cls["abstract"] = True
                cls["is_a"] = head_name
        except Exception:
            pass

        # Check if this element extends a complex type
        if hasattr(elem_def, 'type') and hasattr(elem_def.type, 'content'):
            type_content = elem_def.type.content
            if hasattr(type_content, 'base_type') and type_content.base_type:
                base_type = type_content.base_type
                if hasattr(base_type, 'name'):
                    base_name = str(base_type.name).split("}")[-1]
                    if base_name in linkml_schema["classes"]:
                        cls["is_a"] = base_name
                        
                        # No per-class attributes map in LinkML; rely on slots/slot_usage

        # Add child element slots (content model)
        if not processed_inline_type:
            try:
                if elem_type is not None and hasattr(elem_type, 'content'):
                    content = elem_type.content
                    if hasattr(content, "elements"):
                        iterator = content.elements.items()
                    elif hasattr(content, "iter_elements"):
                        iterator = ((getattr(child, "name", None), child) for child in content.iter_elements())
                    else:
                        iterator = []
                    for child_qname, child in iterator:
                        _add_child_element(element_name, child_qname, child)
                    _collect_choice_constraints(element_name, content)
            except Exception:
                pass
        _apply_local_identities(element_name, elem_def)
        
    # Add properties and attributes from JSON schema
    for prop_name, prop_def in json_schema.get("properties", {}).items():
        if prop_name not in linkml_schema["classes"]:
            continue
            
        # Process attributes in the property
        if "properties" in prop_def:
            required_items = set(prop_def.get("required", []))
            for raw_name, attr_def in prop_def["properties"].items():
                is_attribute = raw_name.startswith('@')
                cleaned_name = raw_name[1:] if is_attribute else raw_name
                slot_def: Dict[str, Union[str, bool]] = {}
                range_hint = _derive_range_from_json_schema(attr_def)
                if range_hint:
                    slot_def["range"] = range_hint
                else:
                    slot_def["range"] = _map_json_type_to_linkml_type(attr_def.get("type", "string"))
                if isinstance(attr_def.get("enum"), list) and attr_def["enum"]:
                    enum_name = _ensure_enum_for_slot(prop_name, cleaned_name, attr_def["enum"])
                    slot_def["range"] = enum_name
                if attr_def.get("type") == "array":
                    slot_def["multivalued"] = True
                if raw_name in required_items or cleaned_name in required_items:
                    slot_def["required"] = True
                if cleaned_name.lower() == "id" and _class_is_ref_like(prop_name):
                    type_hint = attr_def.get("xsdType") or attr_def.get("xsdBaseType")
                    type_hint_local = _local_name(type_hint)
                    target_candidate = _reference_target_for_class(prop_name, type_hint_local)
                    if target_candidate:
                        slot_def["range"] = target_candidate
                _apply_doc_metadata(slot_def, attr_def.get("description"))
                xsd_type_name = str(attr_def.get("xsdType", ""))
                xsd_base_name = str(attr_def.get("xsdBaseType", ""))
                if is_attribute:
                    if cleaned_name.lower() == 'id' or xsd_type_name.endswith('ID') or xsd_base_name.endswith('ID'):
                        slot_def["identifier"] = True
                    if xsd_type_name.endswith('IDREFS') or xsd_base_name.endswith('IDREFS'):
                        slot_def["multivalued"] = True
                _add_attribute(prop_name, cleaned_name, slot_def)
    
    # Map xsd:key / xsd:unique definitions to LinkML unique_keys
    identities = getattr(xsd, "identities", None)
    key_target_map: Dict[str, set] = {}
    if identities:
        for ident_name, ident in identities.items():
            if isinstance(ident, (XsdKey, XsdUnique)):
                selector = getattr(ident, "selector", None)
                if selector is None:
                    continue
                selector_path = getattr(selector, "path", "") or ""
                selector_paths = [p.strip() for p in selector_path.split("|")] if selector_path else []
                if not selector_paths:
                    selector_paths = [selector_path]
                slot_names: List[str] = []
                for field in getattr(ident, "fields", []) or []:
                    field_path = getattr(field, "path", "") or ""
                    if not field_path:
                        continue
                    field_segment = field_path.split("/")[-1]
                    if field_segment.startswith("@"):
                        field_segment = field_segment[1:]
                    field_segment = _local_name(field_segment)
                    if field_segment:
                        slot_names.append(field_segment)
                if not slot_names:
                    continue
                key_local_name = _local_name(ident_name)
                for path in selector_paths:
                    path = path.strip()
                    if not path:
                        continue
                    segments = [seg for seg in path.replace("//", "/").split("/") if seg and seg != "."]
                    if not segments:
                        continue
                    target_segment = segments[-1]
                    if target_segment in ("*", "."):
                        continue
                    target_class = _local_name(target_segment)
                    if not target_class:
                        continue
                    key_target_map.setdefault(key_local_name, set()).add(target_class)
                    _add_unique_key(target_class, key_local_name, slot_names)
            elif isinstance(ident, XsdKeyref):
                refer = getattr(ident, "refer", None)
                if refer is None:
                    continue
                refer_name = getattr(refer, "name", None)
                refer_key = _local_name(refer_name) if refer_name else _local_name(refer)
                target_classes = key_target_map.get(refer_key) or set()
                selector = getattr(ident, "selector", None)
                selector_path = getattr(selector, "path", "") if selector else ""
                segments = [seg for seg in selector_path.replace("//", "/").split("/") if seg and seg != "."]
                if not segments:
                    continue
                source_segment = segments[-1]
                if source_segment in ("*", "."):
                    continue
                source_class = _local_name(source_segment)
                range_target = _select_keyref_range(target_classes)
                for field in getattr(ident, "fields", []) or []:
                    field_path = getattr(field, "path", "") or ""
                    if not field_path:
                        continue
                    field_segment = field_path.split("/")[-1]
                    if field_segment.startswith("@"):
                        field_segment = field_segment[1:]
                    slot_name = _local_name(field_segment)
                    if not slot_name or not range_target:
                        continue
                    slot_def = {
                        "range": range_target,
                        "annotations": {
                            f"references_{range_target}": {
                                "tag": "references",
                                "value": range_target
                            }
                        }
                    }
                    _add_attribute(source_class, slot_name, slot_def)

    # Relax required/multiplicity for slots participating in choices
    for class_name, slot_names in choice_slot_membership.items():
        cls = linkml_schema["classes"].get(class_name)
        if not cls:
            continue
        attrs = cls.get("attributes", {})
        for slot_name in slot_names:
            slot = attrs.get(slot_name)
            if not slot:
                continue
            slot.pop("required", None)
    for class_name, slot_names in choice_repeat_membership.items():
        cls = linkml_schema["classes"].get(class_name)
        if not cls:
            continue
        attrs = cls.get("attributes", {})
        for slot_name in slot_names:
            slot = attrs.get(slot_name)
            if not slot:
                continue
            slot["multivalued"] = True
    def _collect_ancestor_attributes(class_name: str) -> Dict[str, Dict]:
        collected: Dict[str, Dict] = {}
        visited: set = set()
        current = linkml_schema["classes"].get(class_name, {}).get("is_a")
        while current and current not in visited:
            visited.add(current)
            base_cls = linkml_schema["classes"].get(current)
            if not base_cls:
                break
            for attr_name, attr_def in base_cls.get("attributes", {}).items():
                if attr_name not in collected:
                    collected[attr_name] = attr_def
            current = base_cls.get("is_a")
        return collected

    for class_name, cls in linkml_schema["classes"].items():
        base_attrs = _collect_ancestor_attributes(class_name)
        if not base_attrs:
            continue
        attrs = cls.get("attributes")
        if not attrs:
            continue
        for attr_name in list(attrs.keys()):
            if attr_name in base_attrs:
                attrs.pop(attr_name, None)

    # Drop empty enums block
    if "enums" in linkml_schema and not linkml_schema["enums"]:
        del linkml_schema["enums"]
    if not linkml_schema.get("slots"):
        linkml_schema.pop("slots", None)
    
    return _ensure_schema_serializable(linkml_schema)

def _get_documentation(annotation):
    """
    Extract documentation from an XSD annotation.
    
    Args:
        annotation: The XSD annotation object
        
    Returns:
        Documentation string or None
    """
    if not annotation:
        return None
    texts = []
    # 1) xmlschema exposes .documentation sometimes as element(s)/str
    try:
        if hasattr(annotation, 'documentation'):
            doc = annotation.documentation
            if doc:
                texts.append(_extract_text(doc))
    except Exception:
        pass
    # 2) Walk children of the raw XML element
    try:
        if hasattr(annotation, 'elem') and annotation.elem is not None:
            for child in annotation.elem:
                if str(child.tag).endswith('documentation'):
                    texts.append((child.text or "").strip())
    except Exception:
        pass
    # 3) XPath search as a fallback
    try:
        if hasattr(annotation, 'elem') and hasattr(annotation.elem, 'findall'):
            doc_elems = annotation.elem.findall('.//{*}documentation')
            for de in doc_elems:
                texts.append((de.text or "").strip())
    except Exception:
        pass
    # 4) Fallback: regex from string form
    if not texts:
        try:
            annotation_str = str(annotation)
            if 'documentation' in annotation_str:
                import re
                match = re.search(r'<documentation>(.*?)</documentation>', annotation_str, re.DOTALL)
                if match:
                    texts.append(match.group(1).strip())
        except Exception:
            pass
    merged = "\n".join([t for t in texts if t])
    return merged or None

def _add_common_base_classes(linkml_schema, xsd):
    return None

def _map_json_type_to_linkml_type(json_type):
    """Map JSON Schema types to LinkML types"""
    type_map = {
        "string": "string",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        # Treat JSON object/array as strings in LinkML unless mapped to classes/enums
        "object": "string",
        "array": "string"
    }
    return type_map.get(json_type, "object")

def _derive_range_from_json_schema(prop_schema: Dict) -> Optional[str]:
    if not isinstance(prop_schema, dict):
        return None
    if "$ref" in prop_schema:
        return prop_schema["$ref"].split("/")[-1]
    if "allOf" in prop_schema:
        for candidate in prop_schema.get("allOf", []):
            rng = _derive_range_from_json_schema(candidate)
            if rng:
                return rng
    prop_type = prop_schema.get("type")
    if prop_type == "array":
        items = prop_schema.get("items", {})
        return _derive_range_from_json_schema(items)
    if prop_type == "object":
        return None
    return _map_json_type_to_linkml_type(prop_type)

def _ensure_serializable(value):
    """
    Ensure a value is serializable to YAML.
    
    Args:
        value: The value to check
        
    Returns:
        A YAML-serializable version of the value
    """
    # Convert None to empty string to avoid downstream CLI errors
    if value is None:
        return ""
    # Handle Element objects from the lxml or xml.etree modules
    if hasattr(value, 'tag') and hasattr(value, 'text'):
        return value.text.strip() if value.text else ""
    
    # Return other primitives as is
    return str(value) if not isinstance(value, (str, int, float, bool, list, dict, type(None))) else value

def _ensure_schema_serializable(schema):
    """
    Ensure all values in the schema are serializable to YAML.
    
    Args:
        schema: The schema to check
        
    Returns:
        A YAML-serializable version of the schema
    """
    if isinstance(schema, dict):
        # Drop keys with None values; convert others
        return {k: _ensure_schema_serializable(v) for k, v in schema.items() if v is not None}
    elif isinstance(schema, list):
        return [_ensure_schema_serializable(item) for item in schema]
    else:
        return _ensure_serializable(schema)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate LinkML schema from an XSD")
    parser.add_argument("xsd_path", help="Path to the XSD file")
    parser.add_argument("--output", help="Output path for the LinkML schema")
    parser.add_argument("--elements", help="Comma-separated list of top-level elements to include")
    parser.add_argument("--partition", action="store_true", help="Partition the schema into separate files")
    parser.add_argument("--schema-id", help="Override schema id (default derives from target namespace)")
    parser.add_argument("--name", dest="schema_name", help="Override schema name (default derives from namespace)")
    parser.add_argument("--title", dest="schema_title", help="Override schema title")
    parser.add_argument("--default-prefix", dest="default_prefix", help="Override default prefix")
    parser.add_argument("--extra-prefix", action='append', default=[], help="Extra prefix mapping in form prefix=URI; can be repeated")
    parser.add_argument("--json-out", dest="json_output", help="Optional path to write the intermediate JSON Schema")
    parser.add_argument("--doc-overrides", dest="doc_overrides", help="Path to YAML file with documentation overrides")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    top_level_elements = args.elements.split(",") if args.elements else None
    # Parse extra prefixes
    extra_prefixes = {}
    for item in args.extra_prefix:
        try:
            k, v = item.split('=', 1)
            extra_prefixes[k.strip()] = v.strip()
        except ValueError:
            logger.warning(f"Invalid --extra-prefix value (expected prefix=URI): {item}")
    
    generate_linkml_schema(
        args.xsd_path,
        args.output,
        top_level_elements,
        args.partition,
        schema_id=args.schema_id,
        schema_name=args.schema_name,
        schema_title=args.schema_title,
        default_prefix=args.default_prefix,
        extra_prefixes=extra_prefixes if extra_prefixes else None,
        json_output_path=args.json_output,
        doc_overrides_path=args.doc_overrides,
    )