import argparse
import os
import xmlschema
from typing import Dict, List, Optional, Union
import yaml
from pathlib import Path
import json
import logging
from collections import defaultdict

# Fix import for both module and direct script usage
try:
    from src.xsdtojson import xsd_to_json_schema
except ImportError:
    from xsdtojson import xsd_to_json_schema
from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.dumpers import yaml_dumper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_linkml_schema(ome_xsd_path, output_path=None, top_level_elements=None, partition=False, schema_id=None, schema_name=None, schema_title=None, default_prefix=None, extra_prefixes=None, json_output_path=None):
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
        linkml_schema = convert_json_schema_to_linkml(json_schema, xsd, metadata)
        
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
                    for slot_name in class_slots:
                        if slot_name in linkml_schema["slots"]:
                            partitioned_schema["slots"][slot_name] = linkml_schema["slots"][slot_name]

                    # Naive inclusion of classes referenced by slot ranges
                    referenced_classes = set()
                    for slot_name in class_slots:
                        slot_def = linkml_schema["slots"].get(slot_name, {})
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

def convert_json_schema_to_linkml(json_schema, xsd, metadata=None):
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
    linkml_schema = {
        "id": schema_id_val,
        "name": schema_name_val,
        "title": schema_title_val,
        "description": "LinkML translation of the provided XML Schema",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "version": "0.0.1",
        "prefixes": prefixes_val,
        "default_prefix": default_prefix_val,
        "types": {
            "string": {
                "uri": "xsd:string",
                "description": "A character string"
            },
            "integer": {
                "uri": "xsd:integer",
                "description": "An integer"
            },
            "boolean": {
                "uri": "xsd:boolean",
                "description": "A binary (true or false) value"
            },
            "float": {
                "uri": "xsd:float",
                "description": "A floating point number"
            },
            "date": {
                "uri": "xsd:date",
                "description": "A date"
            },
            "datetime": {
                "uri": "xsd:dateTime",
                "description": "A date and time"
            }
        },
        "classes": {},
        "slots": {},
        "enums": {}
    }

    def _coerce_description(value):
        try:
            if value is None:
                return ""
            if isinstance(value, list):
                return "\n".join(str(v) for v in value if v is not None)
            if isinstance(value, dict):
                import json as _json
                return _json.dumps(value, ensure_ascii=False)
            return str(value)
        except Exception:
            return str(value)
    
    # Get complex type hierarchy from XSD
    complex_types = {}
    inheritance_map = {}
    
    # Extract complex types and their inheritance
    for type_name, type_def in xsd.types.items():
        if type_def.is_complex() and not type_name.startswith('{'):
            complex_types[type_name] = type_def
            # Check for base types (inheritance)
            if hasattr(type_def, 'content') and hasattr(type_def.content, 'base_type'):
                base_type = type_def.content.base_type
                if base_type and hasattr(base_type, 'name') and base_type.name:
                    base_name = base_type.name.split("}")[-1]  # Remove namespace
                    inheritance_map[type_name] = base_name
    
    # Create a class for each complex type
    for type_name, type_def in complex_types.items():
        if type_name in linkml_schema["classes"]:
            continue
            
        linkml_schema["classes"][type_name] = {
            "description": f"Complex type {type_name}",
            "slots": []
        }
        
        # Get documentation if available
        if hasattr(type_def, 'annotation') and type_def.annotation:
            doc = _get_documentation(type_def.annotation)
            if doc:
                linkml_schema["classes"][type_name]["description"] = _coerce_description(doc)
        
        # Add inheritance (is_a)
        if type_name in inheritance_map:
            base_type = inheritance_map[type_name]
            if base_type in linkml_schema["classes"]:
                linkml_schema["classes"][type_name]["is_a"] = base_type

        # Add child element slots for complex type content
        try:
            if hasattr(type_def, 'content') and hasattr(type_def.content, 'elements'):
                for child_qname, child in type_def.content.elements.items():
                    child_name = str(child_qname).split("}")[-1]
                    slot_name = child_name
                    child_range = "object"
                    if hasattr(child, 'type') and child.type is not None:
                        if hasattr(child.type, 'name') and child.type.name:
                            child_range = str(child.type.name).split("}")[-1]
                        elif getattr(child.type, 'is_complex', lambda: False)():
                            child_range = child_name
                    if slot_name not in linkml_schema["slots"]:
                        slot_def = {
                            "description": f"Child element {child_name} of {type_name}",
                            "range": child_range
                        }
                        mino = getattr(child, 'min_occurs', None)
                        maxo = getattr(child, 'max_occurs', None)
                        if maxo == 'unbounded' or (isinstance(maxo, int) and maxo > 1):
                            slot_def["multivalued"] = True
                        if isinstance(mino, int) and mino >= 1:
                            slot_def["required"] = True
                        linkml_schema["slots"][slot_name] = slot_def
                    if slot_name not in linkml_schema["classes"][type_name]["slots"]:
                        linkml_schema["classes"][type_name]["slots"].append(slot_name)
        except Exception:
            pass
        
    # Helper to create an enum from a list of values and return enum name
    def _ensure_enum_for_slot(class_name: str, attr_name: str, values: list) -> str:
        import re as _re
        base = f"Enum_{class_name}_{attr_name}"
        enum_name = _re.sub(r"[^A-Za-z0-9_]+", "_", base)
        if enum_name not in linkml_schema["enums"]:
            linkml_schema["enums"][enum_name] = {
                "permissible_values": {str(v): {} for v in values}
            }
        return enum_name

    # Process elements to create classes and slots
    for elem_name, elem_def in xsd.elements.items():
        element_name = str(elem_name).split("}")[-1]  # Remove namespace safely
        
        if element_name in linkml_schema["classes"]:
            continue
            
        # Create class for element
        linkml_schema["classes"][element_name] = {
            "description": f"The {element_name} element from the XML Schema.",
            "slots": []
        }
        
        # Get documentation if available
        if hasattr(elem_def, 'annotation') and elem_def.annotation:
            doc = _get_documentation(elem_def.annotation)
            if doc:
                linkml_schema["classes"][element_name]["description"] = _coerce_description(doc)
        
        # Mark abstract if applicable
        try:
            if getattr(elem_def, 'abstract', False) or getattr(elem_def, 'is_abstract', False):
                linkml_schema["classes"][element_name]["abstract"] = True
        except Exception:
            pass

        # Direct type reference: if element is declared with a named complex type
        try:
            if hasattr(elem_def, 'type') and hasattr(elem_def.type, 'name') and elem_def.type.name:
                direct_type_name = str(elem_def.type.name).split("}")[-1]
                if direct_type_name in linkml_schema["classes"] and direct_type_name != element_name:
                    linkml_schema["classes"][element_name]["is_a"] = direct_type_name
        except Exception:
            pass

        # Substitution group: model as is_a to the head element class and mark head abstract
        try:
            sg = getattr(elem_def, 'substitution_group', None)
            if sg:
                head_name = str(sg).split("}")[-1]
                if head_name not in linkml_schema["classes"]:
                    linkml_schema["classes"][head_name] = {
                        "description": f"Head of substitution group {head_name}",
                        "slots": [],
                        "abstract": True
                    }
                linkml_schema["classes"][element_name]["is_a"] = head_name
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
                        linkml_schema["classes"][element_name]["is_a"] = base_name
                        
                        # Inherit slots from base class
                        base_slots = linkml_schema["classes"][base_name].get("slots", [])
                        linkml_schema["classes"][element_name]["slots"].extend(base_slots)
                        
                        # No per-class attributes map in LinkML; rely on slots/slot_usage

        # Add child element slots (content model)
        try:
            if hasattr(elem_def, 'type') and hasattr(elem_def.type, 'content') and hasattr(elem_def.type.content, 'elements'):
                for child_qname, child in elem_def.type.content.elements.items():
                    child_name = str(child_qname).split("}")[-1]
                    slot_name = child_name
                    # Determine range
                    child_range = "object"
                    if hasattr(child, 'type') and child.type is not None:
                        if hasattr(child.type, 'name') and child.type.name:
                            child_range = str(child.type.name).split("}")[-1]
                        elif getattr(child.type, 'is_complex', lambda: False)():
                            child_range = child_name
                    # Create slot if missing
                    if slot_name not in linkml_schema["slots"]:
                        slot_def = {
                            "description": f"Child element {child_name} of {element_name}",
                            "range": child_range
                        }
                        mino = getattr(child, 'min_occurs', None)
                        maxo = getattr(child, 'max_occurs', None)
                        if isinstance(maxo, int) and maxo > 1:
                            slot_def["multivalued"] = True
                        if maxo == 'unbounded' or (isinstance(maxo, int) and maxo > 1):
                            slot_def["multivalued"] = True
                        if isinstance(mino, int) and mino >= 1:
                            slot_def["required"] = True
                        linkml_schema["slots"][slot_name] = slot_def
                    # Attach slot to class
                    if slot_name not in linkml_schema["classes"][element_name]["slots"]:
                        linkml_schema["classes"][element_name]["slots"].append(slot_name)
        except Exception:
            pass
        
    # Add properties and attributes from JSON schema
    for prop_name, prop_def in json_schema.get("properties", {}).items():
        if prop_name not in linkml_schema["classes"]:
            continue
            
        # Process attributes in the property
        if "properties" in prop_def:
            for attr_name, attr_def in prop_def["properties"].items():
                # Skip attributes that start with @ (these are XML attributes)
                if attr_name.startswith('@'):
                    attr_name = attr_name[1:]  # Remove @ prefix
                
                slot_name = f"attr_{attr_name.lower()}"
                
                # Check if slot already exists
                if slot_name not in linkml_schema["slots"]:
                    slot_range = _map_json_type_to_linkml_type(attr_def.get("type", "string"))
                    xsd_type_name = str(attr_def.get("xsdType", ""))
                    xsd_base_name = str(attr_def.get("xsdBaseType", ""))
                    # Enumerations → create LinkML enum and set as range
                    if isinstance(attr_def, dict) and "enum" in attr_def and isinstance(attr_def["enum"], list) and len(attr_def["enum"]) > 0:
                        enum_name = _ensure_enum_for_slot(prop_name, attr_name, attr_def["enum"])
                        slot_range = enum_name
                    linkml_schema["slots"][slot_name] = {
                        "description": f"Attribute {attr_name} of {prop_name}",
                        "range": slot_range
                    }
                    
                    # Add documentation if available
                    if "description" in attr_def:
                        linkml_schema["slots"][slot_name]["description"] = _coerce_description(attr_def["description"])
                    
                    # Identifier heuristic: ID on non-Ref classes or XSD base ID
                    if (attr_name.lower() == 'id' and not prop_name.endswith('Ref')) or xsd_type_name.endswith('ID') or xsd_base_name.endswith('ID'):
                        linkml_schema["slots"][slot_name]["identifier"] = True
                    # IDREF/IDREFS → reference-like; treat IDREFS as multivalued
                    if xsd_type_name.endswith('IDREFS') or xsd_base_name.endswith('IDREFS'):
                        linkml_schema["slots"][slot_name]["multivalued"] = True
                    
                    # Add required flag
                    if "required" in prop_def and attr_name in prop_def["required"]:
                        linkml_schema["slots"][slot_name]["required"] = True
                
                # Add slot to class
                if slot_name not in linkml_schema["classes"][prop_name]["slots"]:
                    linkml_schema["classes"][prop_name]["slots"].append(slot_name)
    
    # No additional hardcoded base classes; rely solely on parsed XSD content
    
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
        
    # Try to access documentation via different methods
    try:
        # Method 1: Try directly accessing documentation
        if hasattr(annotation, 'documentation'):
            return annotation.documentation
            
        # Method 2: Try to access annotation documentation via children/elements
        if hasattr(annotation, 'elem') and annotation.elem is not None:
            for child in annotation.elem:
                if child.tag.endswith('documentation'):
                    return child.text.strip() if child.text else ""
                    
        # Method 3: Try accessing via lxml attributes
        if hasattr(annotation, 'elem') and hasattr(annotation.elem, 'findall'):
            doc_elems = annotation.elem.findall('.//{*}documentation')
            if doc_elems and len(doc_elems) > 0:
                return doc_elems[0].text.strip() if doc_elems[0].text else ""
                
        # Method 4: Try direct XML parsing if available in the schema
        if hasattr(annotation, 'schema') and hasattr(annotation.schema, 'xpath'):
            doc_nodes = annotation.schema.xpath('.//xs:documentation', namespaces={'xs': 'http://www.w3.org/2001/XMLSchema'})
            if doc_nodes and len(doc_nodes) > 0:
                return doc_nodes[0].text.strip() if doc_nodes[0].text else ""
                
    except Exception as e:
        logger.debug(f"Error extracting documentation: {str(e)}")
        
    # If all else fails, try to extract from string representation
    try:
        annotation_str = str(annotation)
        if 'documentation' in annotation_str:
            import re
            match = re.search(r'<documentation>(.*?)</documentation>', annotation_str, re.DOTALL)
            if match:
                return match.group(1).strip()
    except Exception:
        pass
        
    return None

def _add_common_base_classes(linkml_schema, xsd):
    return None

def _map_json_type_to_linkml_type(json_type):
    """Map JSON Schema types to LinkML types"""
    type_map = {
        "string": "string",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        "object": "object",
        "array": "array"
    }
    return type_map.get(json_type, "object")

def _ensure_serializable(value):
    """
    Ensure a value is serializable to YAML.
    
    Args:
        value: The value to check
        
    Returns:
        A YAML-serializable version of the value
    """
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
        return {k: _ensure_schema_serializable(v) for k, v in schema.items()}
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
    )