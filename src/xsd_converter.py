from typing import Dict, List, Optional, Set
import re
import xmlschema

try:
    from .utils import local_name, ensure_schema_serializable, derive_range_from_json_schema, map_json_type_to_linkml_type
    from .documentation import apply_doc_metadata
    from .reference_resolver import ReferenceResolver
    from .slot_builder import SlotBuilder
    from .constraint_handler import ChoiceConstraintHandler
    from .identity_processor import IdentityProcessor
    from .type_processor import TypeProcessor
except ImportError:
    from utils import local_name, ensure_schema_serializable, derive_range_from_json_schema, map_json_type_to_linkml_type
    from documentation import apply_doc_metadata
    from reference_resolver import ReferenceResolver
    from slot_builder import SlotBuilder
    from constraint_handler import ChoiceConstraintHandler
    from identity_processor import IdentityProcessor
    from type_processor import TypeProcessor


class LinkMLConverter:
    def __init__(self, json_schema: Dict, xsd: xmlschema.XMLSchema, 
                 metadata: Optional[Dict] = None, doc_overrides: Optional[Dict] = None):
        self.json_schema = json_schema
        self.xsd = xsd
        self.metadata = metadata or {}
        self.doc_overrides = doc_overrides or {}
        self.attr_description_overrides = self.doc_overrides.get("attribute_descriptions", {})
        
        self.linkml_schema: Dict = {}
        self.inheritance_map: Dict[str, str] = {}
        self.known_class_names: Set[str] = set()
        
        self._initialize_schema()
        self._collect_known_classes()
        self._build_inheritance_map()
        
        self.reference_resolver = ReferenceResolver(self.linkml_schema, self.inheritance_map, self.known_class_names)
        self.constraint_handler = ChoiceConstraintHandler(self.linkml_schema)
        self.slot_builder = SlotBuilder(self.linkml_schema, self.reference_resolver, self.attr_description_overrides)
        self.type_processor = TypeProcessor(
            self.linkml_schema, 
            self.slot_builder, 
            self.constraint_handler,
            self._ensure_class,
            self._add_attribute
        )
        self.identity_processor = IdentityProcessor(
            self.linkml_schema,
            self.reference_resolver,
            self._ensure_class,
            self._add_attribute,
            self._add_unique_key
        )
    
    def _initialize_schema(self):
        target_ns = getattr(self.xsd, 'target_namespace', None)
        inferred_prefix = self._infer_prefix(target_ns)
        
        schema_id_val = self.metadata.get("schema_id") or (
            f"{target_ns.rstrip('/')}/linkml" if target_ns else "https://w3id.org/linkml/schema"
        )
        schema_name_val = self.metadata.get("schema_name") or inferred_prefix
        schema_title_val = self.metadata.get("schema_title") or f"{schema_name_val.upper()} Schema"
        default_prefix_val = self.metadata.get("default_prefix") or schema_name_val
        
        prefixes_val = {
            "linkml": "https://w3id.org/linkml/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            schema_name_val: (str(target_ns) if target_ns else f"https://example.org/{schema_name_val}#"),
            "schema": "http://schema.org/"
        }
        for k, v in (self.metadata.get("extra_prefixes") or {}).items():
            prefixes_val[k] = v
        
        self.linkml_schema = {
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
            "classes": {},
            "slots": {},
            "enums": {}
        }
    
    def _infer_prefix(self, target_ns) -> str:
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
        
        if not inferred_prefix and target_ns:
            if 'openmicroscopy' in target_ns:
                inferred_prefix = 'ome'
            elif 'bina' in target_ns or 'microscopy' in target_ns:
                inferred_prefix = 'nbo'
        
        if not inferred_prefix:
            inferred_prefix = 'schema'
        
        try:
            if not re.match(r'^[A-Za-z_]', inferred_prefix):
                inferred_prefix = f"ns_{inferred_prefix}"
        except Exception:
            if not (inferred_prefix[:1].isalpha() or inferred_prefix[:1] == '_'):
                inferred_prefix = f"ns_{inferred_prefix}"
        
        return inferred_prefix
    
    def _collect_known_classes(self):
        for elem_name in getattr(self.xsd, "elements", {}):
            local = local_name(elem_name)
            if local:
                self.known_class_names.add(local)
        
        for type_name, type_def in getattr(self.xsd, "types", {}).items():
            if isinstance(type_name, str):
                local = local_name(type_name)
            else:
                local = local_name(getattr(type_def, "name", None))
            if local:
                self.known_class_names.add(local)
    
    def _build_inheritance_map(self):
        for type_name, type_def in self.xsd.types.items():
            if not type_def.is_complex():
                continue
            
            local = local_name(type_name) if isinstance(type_name, str) else local_name(getattr(type_def, "name", None))
            if not local:
                continue
            
            if hasattr(type_def, 'content') and hasattr(type_def.content, 'base_type'):
                base_type = type_def.content.base_type
                if base_type and hasattr(base_type, 'name') and base_type.name:
                    base_name = local_name(base_type.name)
                    if base_name:
                        self.inheritance_map[local] = base_name
    
    def convert(self) -> Dict:
        self.type_processor.process_complex_types(self.xsd, self.inheritance_map)
        self.type_processor.process_elements(self.xsd, self.inheritance_map, self.identity_processor)
        self._process_json_schema_properties()
        self.identity_processor.process_identities(self.xsd)
        self.constraint_handler.relax_choice_constraints()
        self._remove_inherited_attributes()
        self._cleanup_schema()
        
        return ensure_schema_serializable(self.linkml_schema)
    
    def _ensure_class(self, name: str, default_description: Optional[str] = None) -> Dict:
        cls = self.linkml_schema["classes"].setdefault(name, {})
        if default_description:
            if not cls.get("description"):
                cls["description"] = default_description
        cls.setdefault("attributes", {})
        return cls
    
    def _merge_slot(self, existing: Dict, incoming: Dict) -> Dict:
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
    
    def _add_attribute(self, class_name: str, slot_name: str, slot_definition: Dict):
        cls = self._ensure_class(class_name)
        attrs = cls.setdefault("attributes", {})
        if slot_name in attrs:
            attrs[slot_name] = self._merge_slot(attrs[slot_name], slot_definition)
        else:
            attrs[slot_name] = slot_definition
    
    def _add_unique_key(self, class_name: str, key_name: str, slot_names: List[str]):
        if not slot_names:
            return
        cls = self._ensure_class(class_name)
        unique_keys = cls.setdefault("unique_keys", {})
        entry = unique_keys.setdefault(key_name, {"unique_key_slots": []})
        for slot in slot_names:
            if slot not in entry["unique_key_slots"]:
                entry["unique_key_slots"].append(slot)
    
    def _process_json_schema_properties(self):
        for prop_name, prop_def in self.json_schema.get("properties", {}).items():
            if prop_name not in self.linkml_schema["classes"]:
                continue
            
            if "properties" in prop_def:
                required_items = set(prop_def.get("required", []))
                for raw_name, attr_def in prop_def["properties"].items():
                    is_attribute = raw_name.startswith('@')
                    cleaned_name = raw_name[1:] if is_attribute else raw_name
                    slot_def: Dict[str, any] = {}
                    
                    range_hint = derive_range_from_json_schema(attr_def)
                    if range_hint:
                        slot_def["range"] = range_hint
                    else:
                        slot_def["range"] = map_json_type_to_linkml_type(attr_def.get("type", "string"))
                    
                    if isinstance(attr_def.get("enum"), list) and attr_def["enum"]:
                        enum_name = self.slot_builder.ensure_enum_for_slot(prop_name, cleaned_name, attr_def["enum"])
                        slot_def["range"] = enum_name
                    
                    if attr_def.get("type") == "array":
                        slot_def["multivalued"] = True
                    
                    if raw_name in required_items or cleaned_name in required_items:
                        slot_def["required"] = True
                    
                    if cleaned_name.lower() == "id" and self.reference_resolver.class_is_ref_like(prop_name):
                        type_hint = attr_def.get("xsdType") or attr_def.get("xsdBaseType")
                        type_hint_local = local_name(type_hint)
                        target_candidate = self.reference_resolver.reference_target_for_class(prop_name, type_hint_local)
                        if target_candidate:
                            slot_def["range"] = target_candidate
                    
                    apply_doc_metadata(slot_def, attr_def.get("description"))
                    
                    xsd_type_name = str(attr_def.get("xsdType", ""))
                    xsd_base_name = str(attr_def.get("xsdBaseType", ""))
                    if is_attribute:
                        if cleaned_name.lower() == 'id' or xsd_type_name.endswith('ID') or xsd_base_name.endswith('ID'):
                            slot_def["identifier"] = True
                        if xsd_type_name.endswith('IDREFS') or xsd_base_name.endswith('IDREFS'):
                            slot_def["multivalued"] = True
                    
                    self._add_attribute(prop_name, cleaned_name, slot_def)
    
    def _remove_inherited_attributes(self):
        def _collect_ancestor_attributes(class_name: str) -> Dict[str, Dict]:
            collected: Dict[str, Dict] = {}
            visited: Set[str] = set()
            current = self.linkml_schema["classes"].get(class_name, {}).get("is_a")
            while current and current not in visited:
                visited.add(current)
                base_cls = self.linkml_schema["classes"].get(current)
                if not base_cls:
                    break
                for attr_name, attr_def in base_cls.get("attributes", {}).items():
                    if attr_name not in collected:
                        collected[attr_name] = attr_def
                current = base_cls.get("is_a")
            return collected
        
        for class_name, cls in self.linkml_schema["classes"].items():
            base_attrs = _collect_ancestor_attributes(class_name)
            if not base_attrs:
                continue
            attrs = cls.get("attributes")
            if not attrs:
                continue
            for attr_name in list(attrs.keys()):
                if attr_name in base_attrs:
                    attrs.pop(attr_name, None)
    
    def _cleanup_schema(self):
        subsets = self.linkml_schema.get("subsets") or {}
        used_subsets = set()
        for cls in (self.linkml_schema.get("classes") or {}).values():
            for s in cls.get("in_subset", []) or []:
                used_subsets.add(s)
            for attr in (cls.get("attributes") or {}).values():
                for s in attr.get("in_subset", []) or []:
                    used_subsets.add(s)
        for s in sorted(used_subsets):
            if s not in subsets:
                subsets[s] = {"description": s}
        if subsets:
            self.linkml_schema["subsets"] = subsets
        else:
            self.linkml_schema.pop("subsets", None)
        if "enums" in self.linkml_schema and not self.linkml_schema["enums"]:
            del self.linkml_schema["enums"]
        if not self.linkml_schema.get("slots"):
            self.linkml_schema.pop("slots", None)
