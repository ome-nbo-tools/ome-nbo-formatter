import os
import json
import argparse
import xmlschema
import logging
import re
from typing import Dict, Optional, Tuple, Any
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _local_name(name):
    try:
        return str(name).split("}")[-1]
    except Exception:
        return str(name) if name is not None else ""

def xsd_to_json_schema(xsd_path: str) -> Dict:
    """
    Convert an XML Schema to JSON Schema
    
    Args:
        xsd_path: Path to the XML Schema file
        
    Returns:
        A JSON Schema as a Python dictionary
    """
    try:
        # Parse the XSD file
        schema = xmlschema.XMLSchema(xsd_path)
        
        # Build type registry ($defs)
        defs: Dict[str, Any] = {}

        def build_type_schema(tdef) -> Dict[str, Any]:
            # Simple types
            if hasattr(tdef, 'is_simple') and tdef.is_simple():
                base_t = getattr(tdef, 'base_type', None)
                base_name = _local_name(base_t.name) if getattr(base_t, 'name', None) else None
                sch: Dict[str, Any] = {"type": _map_xsd_type_to_json_type(base_name or _local_name(getattr(tdef, 'name', '') or 'string'))}
                # Type-level documentation
                try:
                    tdoc = _get_documentation(getattr(tdef, 'annotation', None))
                    if tdoc:
                        sch["description"] = tdoc
                except Exception:
                    pass
                enums = getattr(tdef, 'enumeration', None)
                if enums:
                    sch["enum"] = [str(v) for v in enums]
                return sch
            # Complex types
            obj: Dict[str, Any] = {"type": "object", "properties": {}}
            # Type-level documentation
            try:
                tdoc = _get_documentation(getattr(tdef, 'annotation', None))
                if tdoc:
                    obj["description"] = tdoc
            except Exception:
                pass
            required: list = []

            # Inheritance
            try:
                if hasattr(tdef, 'content') and getattr(tdef.content, 'base_type', None):
                    base = tdef.content.base_type
                    if getattr(base, 'name', None):
                        bname = _local_name(base.name)
                        if bname not in defs:
                            defs[bname] = build_type_schema(base)
                        # Merge base
                        bsch = defs[bname]
                        if isinstance(bsch, dict) and bsch.get('type') == 'object':
                            obj['properties'].update(bsch.get('properties', {}))
                            if 'required' in bsch:
                                required.extend(bsch['required'])
            except Exception:
                pass

            # Attributes
            try:
                for aname, at in getattr(tdef, 'attributes', {}).items():
                    an = _local_name(aname)
                    obj['properties'][f"@{an}"] = _process_attribute(an, at)
                    if getattr(at, 'use', None) == 'required':
                        required.append(f"@{an}")
            except Exception:
                pass

            # Elements content
            try:
                content = getattr(tdef, 'content', None)
                if content is not None:
                    model = getattr(content, 'model', None)
                    if hasattr(content, 'elements') and content.elements:
                        props: Dict[str, Any] = {}
                        for cname, celem in content.elements.items():
                            child_schema, child_required = _element_to_schema(celem)
                            props[_local_name(cname)] = child_schema
                            if child_required:
                                required.append(_local_name(cname))
                        if model == 'choice':
                            obj['oneOf'] = [{"type": "object", "properties": {k: v}} for k, v in props.items()]
                        else:
                            obj['properties'].update(props)
            except Exception as e:
                logger.debug(f"content parse error: {e}")

            if required:
                obj['required'] = sorted(list(set(required)))
            return obj

        def _element_to_schema(elem) -> Tuple[Dict[str, Any], bool]:
            # Resolve ref
            elem_doc = None
            try:
                elem_doc = _get_documentation(getattr(elem, 'annotation', None))
            except Exception:
                pass
            try:
                if getattr(elem, 'ref', None):
                    target = schema.elements.get(getattr(elem.ref, 'name', None))
                    if target is not None:
                        elem = target
                        # if no local doc, use target's doc
                        if not elem_doc:
                            try:
                                elem_doc = _get_documentation(getattr(elem, 'annotation', None))
                            except Exception:
                                pass
            except Exception:
                pass
            # Determine base schema for the element's type
            et = getattr(elem, 'type', None)
            sch: Dict[str, Any]
            if et is not None and getattr(et, 'name', None):
                tname = _local_name(et.name)
                if tname not in defs:
                    defs[tname] = build_type_schema(et)
                if elem_doc:
                    sch = {"allOf": [{"$ref": f"#/$defs/{tname}"}], "description": elem_doc}
                else:
                    sch = {"$ref": f"#/$defs/{tname}"}
            elif et is not None:
                sch = build_type_schema(et)
                if elem_doc and isinstance(sch, dict) and 'description' not in sch:
                    sch['description'] = elem_doc
            else:
                sch = {"type": "object"}
                if elem_doc:
                    sch['description'] = elem_doc

            mino = getattr(elem, 'min_occurs', None)
            maxo = getattr(elem, 'max_occurs', None)
            if maxo == 'unbounded' or (isinstance(maxo, int) and maxo > 1):
                sch = {"type": "array", "items": sch}
            return sch, isinstance(mino, int) and mino >= 1

        # Build root schema
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {},
            "$defs": defs
        }

        for ename, e in schema.elements.items():
            try:
                name = _local_name(ename)
                es, _ = _element_to_schema(e)
                json_schema['properties'][name] = es
            except Exception as ex:
                logger.warning(f"Error processing element {ename}: {ex}")

        # identities as comment
        try:
            idents = getattr(schema, 'identities', {}) or {}
            if idents:
                json_schema.setdefault('$comment', {})
                json_schema['$comment'] = {
                    'keys': [str(k) for k in idents.get('keys', {}).keys()] if isinstance(idents, dict) else []
                }
        except Exception:
            pass

        json_schema = _make_json_serializable(json_schema)
        return json_schema
    except Exception as e:
        logger.error(f"Error converting XSD to JSON Schema: {str(e)}")
        raise

def _extract_element_content(element_name, element_type, schema, json_schema):
    """
    Extract content from an XSD element.
    
    Args:
        element_name: Name of the element
        element_type: Type of the element
        schema: The XMLSchema object
        json_schema: The JSON Schema being built
        
    Returns:
        A dictionary representing the element's content
    """
    # Create a placeholder for the element
    element_content = {
        "type": "object",
        "properties": {},
    }
    
    # Add description if available
    if hasattr(element_type, 'annotation') and element_type.annotation is not None:
        doc = _get_documentation(element_type.annotation)
        if doc:
            element_content["description"] = doc
    
    # Process attributes
    required_props = []
    
    # If the element has a type definition, process its attributes
    if hasattr(element_type, 'type') and element_type.type is not None:
        type_attributes = element_type.type.attributes if hasattr(element_type.type, 'attributes') else {}
        
        for attr_name, attr_type in type_attributes.items():
            attr_name = _local_name(attr_name)
            attr_content = _process_attribute(attr_name, attr_type)
            
            # Add to properties
            element_content["properties"][f"@{attr_name}"] = attr_content
            
            # Check if required
            if attr_type.use == 'required':
                required_props.append(f"@{attr_name}")
    
    # Add required properties if any
    if required_props:
        element_content["required"] = required_props

    # Add enumeration information if available
    if hasattr(element_type, 'type') and element_type.type is not None:
        if hasattr(element_type.type, 'enumeration') and element_type.type.enumeration:
            enums = [str(v) for v in element_type.type.enumeration]
            if enums:
                element_content["enum"] = enums
    
    # If the element is based on a complex type, extract its content
    if hasattr(element_type, 'type') and element_type.type is not None and hasattr(element_type.type, 'is_complex') and element_type.type.is_complex():
        # Process attributes from the type
        try:
            if hasattr(element_type.type, 'attributes'):
                for attr_name, attr_type in element_type.type.attributes.items():
                    attr_name = _local_name(attr_name)
                    attr_content = _process_attribute(attr_name, attr_type)
                    
                    # Add to properties
                    element_content["properties"][f"@{attr_name}"] = attr_content
                    
                    # Check if required
                    if attr_type.use == 'required':
                        if "required" not in element_content:
                            element_content["required"] = []
                        element_content["required"].append(f"@{attr_name}")
        except Exception as e:
            logger.warning(f"Error processing attributes of {element_name}: {str(e)}")
        
        # Process content elements (children)
        try:
            if hasattr(element_type.type, 'content') and element_type.type.content is not None:
                if hasattr(element_type.type.content, 'elements'):
                    for child_name, child_type in element_type.type.content.elements.items():
                        child_name = _local_name(child_name)
                        child_content = {
                            "type": "object",
                            "properties": {}
                        }
                        
                        # Extract child content recursively
                        child_content = _extract_element_content(child_name, child_type, schema, json_schema)
                        
                        # Add to properties
                        element_content["properties"][child_name] = child_content
        except Exception as e:
            logger.warning(f"Error processing content of {element_name}: {str(e)}")
    
    # Check if the element extends a complex type through inheritance
    if hasattr(element_type, 'type') and element_type.type is not None and hasattr(element_type.type, 'content'):
        type_content = element_type.type.content
        if hasattr(type_content, 'base_type') and type_content.base_type is not None:
            base_type = type_content.base_type
            if hasattr(base_type, 'name') and base_type.name is not None:
                base_name = _local_name(base_type.name)
                
                # Add information about the base type
                element_content["baseType"] = base_name
                
                # Process attributes from the base type to include in this element
                try:
                    if hasattr(base_type, 'attributes'):
                        for attr_name, attr_type in base_type.attributes.items():
                            attr_name = _local_name(attr_name)
                            attr_content = _process_attribute(attr_name, attr_type)
                            
                            # Add to properties if not already present
                            if f"@{attr_name}" not in element_content["properties"]:
                                element_content["properties"][f"@{attr_name}"] = attr_content
                                
                                # Check if required
                                if attr_type.use == 'required':
                                    if "required" not in element_content:
                                        element_content["required"] = []
                                    element_content["required"].append(f"@{attr_name}")
                except Exception as e:
                    logger.warning(f"Error processing base type attributes of {element_name}: {str(e)}")
    
    # Extract documentation if available
    if hasattr(element_type, 'annotation') and element_type.annotation is not None:
        doc = _get_documentation(element_type.annotation)
        if doc:
            element_content["description"] = doc
    
    return element_content

def _process_attribute(attr_name, attr_type):
    """
    Process an XSD attribute.
    
    Args:
        attr_name: Name of the attribute
        attr_type: Type of the attribute
        
    Returns:
        A dictionary representing the attribute's content
    """
    # Determine declared and base XSD type names
    declared_type_name = None
    base_type_name = None
    try:
        if hasattr(attr_type, 'type') and hasattr(attr_type.type, 'name'):
            declared_type_name = attr_type.type.name
            # Walk restrictions to primitive base
            t = attr_type.type
            seen = set()
            while hasattr(t, 'base_type') and t.base_type is not None and t.base_type not in seen:
                seen.add(t)
                bt = t.base_type
                if hasattr(bt, 'name') and bt.name:
                    base_type_name = bt.name
                t = bt
    except Exception:
        pass

    attr_content = {
        "type": _map_xsd_type_to_json_type(declared_type_name if declared_type_name else "string")
    }
    # Preserve raw XSD type info for downstream mapping
    if declared_type_name:
        attr_content["xsdType"] = str(declared_type_name)
    if base_type_name:
        attr_content["xsdBaseType"] = str(base_type_name)
    
    # Add description if available
    if hasattr(attr_type, 'annotation') and attr_type.annotation is not None:
        doc = _get_documentation(attr_type.annotation)
        if doc:
            attr_content["description"] = doc
    
    # Add enumeration information if available
    if hasattr(attr_type, 'type') and hasattr(attr_type.type, 'enumeration') and attr_type.type.enumeration:
        enums = [str(v) for v in attr_type.type.enumeration]
        if enums:
            attr_content["enum"] = enums
    
    return attr_content

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

def _map_xsd_type_to_json_type(xsd_type):
    """
    Map an XSD type to a JSON Schema type
    
    Args:
        xsd_type: XSD type name
        
    Returns:
        JSON Schema type
    """
    # Handle None values
    if xsd_type is None:
        return "object"
        
    # Remove namespace if present
    if "}" in xsd_type:
        xsd_type = xsd_type.split("}")[-1]
    
    # Map XSD types to JSON Schema types
    type_map = {
        "string": "string",
        "normalizedString": "string",
        "token": "string",
        "byte": "integer",
        "short": "integer",
        "integer": "integer",
        "int": "integer",
        "long": "integer",
        "unsignedByte": "integer",
        "unsignedShort": "integer",
        "unsignedInt": "integer",
        "unsignedLong": "integer",
        "decimal": "number",
        "float": "number",
        "double": "number",
        "boolean": "boolean",
        "date": "string",
        "dateTime": "string",
        "time": "string",
        "anyURI": "string",
        "ID": "string",
        "IDREF": "string",
        "NMTOKEN": "string",
        "anyType": "object"
    }
    
    return type_map.get(xsd_type, "object")

def _make_json_serializable(obj):
    """
    Make an object JSON serializable by replacing XML Element objects with strings.
    
    Args:
        obj: The object to make serializable
        
    Returns:
        A JSON serializable version of the object
    """
    if obj is None:
        return None
    
    # Handle Element objects from the lxml or xml.etree modules
    if hasattr(obj, 'tag') and hasattr(obj, 'text'):
        return obj.text.strip() if obj.text else ""
    
    # Handle dictionaries
    if isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    
    # Handle lists and tuples
    if isinstance(obj, (list, tuple)):
        return [_make_json_serializable(item) for item in obj]
    
    # Return other primitives as is
    return obj

def main():
    """Command-line interface for xsd_to_json_schema"""
    parser = argparse.ArgumentParser(description="Convert XML Schema to JSON Schema")
    parser.add_argument("input_file", help="Path to the XML Schema file")
    parser.add_argument("--output", "-o", help="Path to write the JSON Schema file")
    
    args = parser.parse_args()
    
    # Convert XSD to JSON Schema
    json_schema = xsd_to_json_schema(args.input_file)
    
    # Output the JSON Schema
    if args.output:
        with open(args.output, "w") as f:
            json.dump(json_schema, f, indent=2)
    else:
        print(json.dumps(json_schema, indent=2))

if __name__ == "__main__":
    main() 