# XSD to LinkML Schema Converter (OME + NBO)

## Overview
Convert arbitrary XML Schema (XSD) into LinkML YAML. The pipeline preserves elements, attributes, inheritance, multiplicity, and documentation. It’s generalized for OME and NBO schemas and works offline once sources are fetched.

## What it generates
- Single LinkML YAML or partitioned per-class files
- Classes for complex types and top-level elements (with is_a inheritance)
- Slots for attributes and child elements (required/multivalued from min/maxOccurs)
- Descriptions from XSD annotations
- Enums from XSD enumerations (where present)
- Identifier slots from XSD ID/IDREF/IDREFS heuristics

## Installation

### Prerequisites
- Python 3.8 or higher
- Git

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd ome-nbo-formatter
```

2. **Create virtual environment**
```bash
python -m venv venv
```

3. **Activate virtual environment**

**Linux/macOS:**
```bash
source venv/bin/activate
```

**Windows:**
```bash
venv\Scripts\activate
```

4. **Install dependencies**
```bash
pip install -r requirements.txt
```

5. **Verify installation**
```bash
python src/generator.py --help
```

### Deactivate virtual environment
```bash
deactivate
```

## Fetch NBO sources on demand
Fetch only required parts of the public NBO repo using git sparse checkout.

```bash
python ome-nbo-formatter/src/fetch_nbo.py \
  --repo-url https://github.com/WU-BIMAC/NBOMicroscopyMetadataSpecs.git \
  --ref master \
  --dest NBOMicroscopyMetadataSpecs \
  --path "Model/stable version/v02-01" \
  --path "Model/in progress/v02-10"
```

## Generate LinkML
```bash
# Full schema
python ome-nbo-formatter/src/generator.py <path/to/schema.xsd> --output out.yaml -v

# Partitioned (one file per class)
python ome-nbo-formatter/src/generator.py <path/to/schema.xsd> --output out_dir --partition -v

# Restrict to specific top-level elements
python ome-nbo-formatter/src/generator.py <path/to/schema.xsd> --output out.yaml --elements Image,Pixels -v
```

Advanced metadata overrides:
```bash
  --schema-id URL            # override schema id
  --name NAME                # override schema name (NCName)
  --title TITLE              # override title
  --default-prefix PREFIX    # override default prefix
  --extra-prefix pfx=URI     # repeatable
```

## Verify structural coverage (XSD vs LinkML)
Compare classes/attributes/children to ensure nothing is lost.

```bash
python ome-nbo-formatter/src/verify_equivalence.py <path/to/schema.xsd> out.yaml
```

## Batch over NBO schemas
Regenerate and verify across all NBO XSDs; auto-fetch if missing.

```bash
python ome-nbo-formatter/src/batch_verify_nbo.py --auto-fetch

# Or explicitly
python ome-nbo-formatter/src/batch_verify_nbo.py \
  --nbo-root NBOMicroscopyMetadataSpecs \
  --out-dir _nbo_batch
```

Output includes a summary table with XSD vs LinkML class counts and mismatches.

## Design
- XSD → JSON model → LinkML. The JSON step normalizes XSD constructs and improves testability.
- Namespace-derived metadata (id/name/prefix) with NCName-safe coercion; overridable via CLI.
- Child model: slots with range, required, multivalued derived from XSD content.
- Enumerations mapped to LinkML enums; identifier heuristic for ID/IDREF(S).

## Current limitations (planned)
- substitutionGroup: head modeled as abstract + is_a; future union_of for families
- key/keyref integrity not emitted yet
- choice/mixed/any not fully modeled (will use union_of/any_of strategies)
- Complex simpleType facets (patterns/min/max/length) partially emitted; will expand
- Multi-namespace include/import normalization improvements pending

## Tests
```bash
python -m pytest
```

## License
MIT (see LICENSE)

## Acknowledgments
- OME / NBO schema authors
- LinkML project

