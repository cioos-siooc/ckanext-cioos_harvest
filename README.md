# ckanext-cioos_harvest

CIOOS-SIOOC plugin that extends the CKAN harvest and spatial harvest
extensions. Its primary purpose is to modify dataset packages during
harvest so they work correctly with the scheming, fluent, and
composite extensions used by CIOOS.

## What It Does

During the harvest import stage, the plugin:

- Transforms ISO 19115-3 metadata to ISO 19115-1 format so it
  works with the spatial harvester's default 19115-1 schemas
- Moves scheming fields from `extras` into the package root
- Ensures list and multi-list scheming field values are stored
  as lists rather than strings
- Places composite and composite-repeating fields into `__extras`
  where the composite extension expects them
- Renames composite fields using the configured separator and
  collapses nested keys into concatenated field names
- Converts fluent tag fields into language dictionaries
- Populates fluent fields with the default language if no
  language dictionary is provided
- Matches and creates organizations and groups from harvested
  metadata

## Harvester Types

This extension provides three harvester types that appear in the
CKAN harvest source creation form:

| Plugin Name | Harvester Class | Description |
| --- | --- | --- |
| `ckan_cioos_harvester` | `CIOOSCKANHarvester` | Harvests remote CKAN instances with improved handling/indexing of external XML files and organization matching |
| `ckan_spatial_harvester` | `CKANSpatialHarvester` | Harvests remote CKAN instances filtering by spatial query (bounding box) |
| `ckan_schema_harvester` | `CIOOSCKANSchemaHarvester` | Harvests remote CKAN instances without requiring a matching schema |

Additional harvesters are provided by
[ckanext-cioos_spatial](../ckanext-cioos_spatial/):

| Plugin Name | Harvester Class | Description |
| --- | --- | --- |
| `cioos_waf_harvester` | `WAFHarvesterCIOOS` | WAF harvester with ISO 19115-3 to ISO 19139 transformation |
| `cioos_datastream_harvester` | `DatastreamSitemapHarvester` | Harvests Datastream sitemap feeds |
| `cioos_geonetwork_harvester` | `GeoNetworkHarvester` | Harvests GeoNetwork catalog instances |

## Requirements

- CKAN 2.11+
- ckanext-harvest
- ckanext-spatial
- ckanext-scheming
- ckanext-fluent

## Configuration

### ckan.ini

Set timeout for `request.get` when reading full XML body from a
URL (used by the CIOOS CKAN harvester):

```ini
ckan.index_xml_url_read_timeout = 500
```

### Harvest Source Config

Per-source configuration is set as JSON in the harvest source
configuration field. All fields are optional.

```json
{
  "url_read_timeout": 500,
  "source_title": "My Source",
  "source_description": "Description of the source"
}
```

## Installation

This extension is installed automatically when mounted into the
CIOOS CKAN Docker development container via the `src/` directory.

For manual installation:

1. Activate your CKAN virtual environment
2. Install the package:

   ```shell
   pip install -e 'git+https://github.com/cioos-siooc/ckanext-cioos_harvest.git#egg=ckanext-cioos_harvest'
   ```

3. Add the plugins to `ckan.plugins` in your CKAN config:

   ```ini
   ckan.plugins = ... cioos_harvest ckan_cioos_harvester ckan_spatial_harvester ckan_schema_harvester
   ```

4. Restart CKAN

## Running Harvests

In the CIOOS CKAN Docker setup, harvests are run by a dedicated
`ckan-harvest-worker` container. See the
[main README](../../README.md#harvest-workers) for usage.
