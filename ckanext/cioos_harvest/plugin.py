import json
import logging
from numbers import Number

import ckan.lib.munge as munge
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.cioos_harvest.harvesters.base import (
    extract_xml_from_harvest_object,
    get_object_extra,
    handle_groups,
    load_json,
    make_site_catalogue_entry,
    translate_resource_fields,
)
from ckanext.cioos_harvest.harvesters.field_handlers import (
    handle_composite_field,
    handle_fluent_field,
    handle_scheming_field,
)
from ckanext.spatial.interfaces import ISpatialHarvester
from ckanext.spatial.validation.validation import BaseValidator

# Re-export harvester classes so that existing imports from plugin.py
# (and setup.py entry points) continue to work.
from ckanext.cioos_harvest.harvesters.ckan_cioos import CIOOSCKANHarvester  # noqa: F401
from ckanext.cioos_harvest.harvesters.ckan_spatial import CKANSpatialHarvester  # noqa: F401

log = logging.getLogger(__name__)


# place holder, spatial extension expects a validator to be present
class MyValidator(BaseValidator):
    name = "my-validator"

    title = "My very own validator"

    @classmethod
    def is_valid(cls, xml):

        return True, []


class Cioos_HarvestPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IActions)
    plugins.implements(ISpatialHarvester, inherit=True)
    plugins.implements(plugins.IOrganizationController, inherit=True)

    # IOrganizationController
    def read(self, entity):
        pass

    def create(self, entity):
        if hasattr(entity, "title_translated"):
            if entity.title_translated == "{}" or not entity.title_translated:
                toolkit.get_action("organization_patch")(
                    data_dict={
                        "id": entity.id,
                        "title": entity.title,
                        "title_translated": '{"en":"%s", "fr":"%s"}'
                        % (entity.title, entity.title),
                    }
                )
        return entity

    def edit(self, entity):
        pass

    def delete(self, entity):
        pass

    def before_dataset_view(self, pkg_dict):
        return pkg_dict

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("fanstatic", "cioos_harvest")

    # ITemplateHelpers
    def get_helpers(self):
        from ckanext.cioos_harvest import helpers as h

        return {
            "cioos_spatial_widget_expands": h.spatial_widget_expands,
            "cioos_spatial_default_extent": h.spatial_default_extent,
            "cioos_spatial_get_map_initial_max_zoom": h.spatial_get_map_initial_max_zoom,
        }

    # IActions
    def get_actions(self):
        from ckanext.cioos_harvest import logic

        return {
            "spatial_query_geo": logic.spatial_query_geo,
            "spatial_query_geo_package_search": logic.spatial_query_geo_package_search,
        }

    # ISpatialHarvester
    def get_validators(self):
        return [MyValidator]

    # Delegate to shared base functions (kept as methods for backward compat)
    from_json = staticmethod(load_json)
    _get_object_extra = staticmethod(get_object_extra)

    def trim_values(self, values):
        if isinstance(values, Number):
            return values
        elif isinstance(values, list):
            return [self.trim_values(x) for x in values]
        elif isinstance(values, dict):
            return {k.strip(): self.trim_values(v) for k, v in values.items()}
        elif isinstance(values, str):
            try:
                json_object = json.loads(values)
            except ValueError:
                return values.strip()
            else:
                return json.dumps(self.trim_values(json_object))
        return values

    def cioos_guess_resource_format(self, url, use_mimetypes=True):
        """
        Given a URL try to guess the best format to assign to the resource

        This function does not replace the guess_resource_format() in the base
        spatial harvester. In stead it adds some resource and file types that
        are missing from that function.

        Returns None if no format could be guessed.

        """
        url = url.lower().strip()
        resource_types = {
            # ERDDAP
            "ERDDAP": ("/erddap/",),
            # OBIS
            "OBIS": ("/ipt.iobis.org/",),
        }

        for resource_type, parts in resource_types.items():
            if any(part in url for part in parts):
                return resource_type

        file_types = {
            "CSV": ("csv",),
            "PDF": ("pdf",),
            "TXT": ("txt",),
            "XML": ("xml",),
            "HTML": ("html",),
            "JSON": ("json",),
        }

        for file_type, extensions in file_types.items():
            if any(url.endswith(extension) for extension in extensions):
                return file_type

        return None

    def get_package_dict(self, context, data_dict):
        package_dict = data_dict["package_dict"]
        iso_values = data_dict["iso_values"]
        harvest_object = data_dict["harvest_object"]
        source_config = json.loads(data_dict["harvest_object"].source.config or "{}")
        xml_location_url = self._get_object_extra(
            data_dict["harvest_object"], "waf_location"
        )
        xml_modified_date = self._get_object_extra(
            data_dict["harvest_object"], "waf_modified_date"
        )

        # convert extras key:value list to dictionary
        extras = {x["key"]: x["value"] for x in package_dict.get("extras", [])}

        extras["xml_location_url"] = xml_location_url
        if xml_modified_date:
            extras["xml_modified_date"] = xml_modified_date.replace("Z", "")

        # copy some fields over from iso_values if they exist
        if iso_values.get("limitations-on-public-access"):
            extras["limitations-on-public-access"] = iso_values.get(
                "limitations-on-public-access"
            )
        if iso_values.get("access-constraints"):
            extras["access-constraints"] = iso_values.get("access-constraints")
        if iso_values.get("use-constraints"):
            extras["use-constraints"] = iso_values.get("use-constraints")
        if iso_values.get("use-constraints-code"):
            extras["use-constraints-code"] = iso_values.get("use-constraints-code")
        if iso_values.get("legal-constraints-reference-code"):
            extras["legal-constraints-reference-code"] = iso_values.get(
                "legal-constraints-reference-code"
            )
        if iso_values.get("distributor"):
            extras["distributor"] = iso_values.get("distributor")
        if iso_values.get("dataset-language"):
            extras["dataset-language"] = iso_values.get("dataset-language")
        if iso_values.get("dataset-language-other"):
            extras["dataset-language-other"] = iso_values.get("dataset-language-other")

        # populate harvest source organization
        harvest_source = toolkit.get_action("harvest_source_show")(
            data_dict={"id": harvest_object.source.id}
        )
        extras["harvest_source_organization"] = harvest_source.get("organization")

        # load remote xml content
        package_dict = extract_xml_from_harvest_object(package_dict, harvest_object)

        # Handle Scheming, Composit, and Fluent extensions
        loaded_plugins = plugins.toolkit.config.get("ckan.plugins")
        if "scheming_datasets" in loaded_plugins:
            # composite = 'composite' in loaded_plugins
            fluent = "fluent" in loaded_plugins
            schema = plugins.toolkit.h.scheming_get_dataset_schema("dataset")

            # Package name: prefer guid over title (reverse of ckanext-spatial default).
            # Read title from iso_values (the raw parsed values), not package_dict['title'],
            # because the harvester's get_package_dict() may have already converted the title
            # to a plain string by the time this ISpatialHarvester callback runs.
            _title_parsed = self.from_json(iso_values.get("title", "{}"))
            title_as_name = (
                _title_parsed.get("en", package_dict["name"])
                if isinstance(_title_parsed, dict)
                else (_title_parsed or package_dict["name"])
            )
            name = munge.munge_name(extras.get("guid", title_as_name)).lower()
            package_dict["name"] = name

            # add uri key for dcat extension to use. this field is used as the
            # dataset id in rdf / jsonld output
            package_uri = toolkit.config.get("ckan.site_url") + "/dataset/" + name
            extras["uri"] = package_uri

            # populate license_id
            if not package_dict.get("license_id"):
                package_dict["license_id"] = (
                    iso_values.get("legal-constraints-reference-code")
                    or iso_values.get("use-constraints")
                    or ""
                )
                if not package_dict["license_id"]:
                    log.warning("No license_id found.")

            # populate citation — inject CKAN dataset URL into each language's CSL-JSON.
            # The parser leaves URL='' because ckan.site_url is only accessible here.
            _citation = iso_values.get("citation")
            if isinstance(_citation, dict) and _citation:
                _pkg_url_base = (
                    toolkit.config.get("ckan.site_url", "").rstrip("/")
                    + "/dataset/"
                    + package_dict.get("name", "")
                )
                _updated_citation = {}
                for _lang, _csl_str in _citation.items():
                    try:
                        _csl = json.loads(_csl_str)
                        if isinstance(_csl, list) and _csl:
                            _csl[0]["URL"] = _pkg_url_base + "?local=" + _lang
                        _updated_citation[_lang] = json.dumps(_csl, ensure_ascii=False)
                    except Exception:
                        _updated_citation[_lang] = _csl_str
                package_dict["citation"] = _updated_citation
            elif _citation is not None:
                package_dict["citation"] = _citation

            # populate projects
            package_dict["projects"] = iso_values.get("keyword-project", [])

            # populate datacentre
            package_dict["datacentre"] = iso_values.get("keyword-datacentre", [])

            # populate lineage - convert string to list of dicts as required by schema
            lineage_value = iso_values.get("lineage", [])
            if isinstance(lineage_value, str):
                # Convert string lineage to required list of dicts format
                log.warning(
                    "Converting lineage from string to list of dicts for dataset: %s",
                    iso_values.get("guid", "unknown"),
                )
                package_dict["lineage"] = [
                    {
                        "statment": {"en": lineage_value},
                        "scope": "dataset",
                        "additional-documentation": [],
                        "source": [],
                        "processing-step": [],
                    }
                ]
            else:
                package_dict["lineage"] = lineage_value if lineage_value else []

            # populate publishing data catalogue list
            package_dict["included_in_data_catalogue"] = [make_site_catalogue_entry()]

            if source_config.get("data_catalogue_source"):
                package_dict["included_in_data_catalogue"] = (
                    load_json(source_config["data_catalogue_source"])
                    + package_dict["included_in_data_catalogue"]
                )

            # Always populate translation_method for bilingual fields with at
            # least both CIOOS portal languages present.  Merge any values the
            # parser may have produced on top of the empty-string defaults.
            _default_tm = {"en": "", "fr": ""}
            package_dict["title_translation_method"] = dict(
                _default_tm, **(iso_values.get("title_translation_method") or {})
            )
            package_dict["notes_translation_method"] = dict(
                _default_tm, **(iso_values.get("abstract_translation_method") or {})
            )
            package_dict["keywords_translation_method"] = dict(
                _default_tm, **(iso_values.get("keywords_translation_method") or {})
            )

            # set default language, default to english
            default_language = iso_values.get("metadata-language", "en")[0:2]
            if not default_language:
                default_language = "en"

            # iterate over schema fields and update package dictionary as needed
            for field in schema["dataset_fields"]:
                handled_fields = []
                self.handle_composite_harvest_dictinary(
                    field,
                    iso_values,
                    extras,
                    package_dict,
                    default_language,
                    handled_fields,
                )

                if fluent:
                    self.handle_fluent_harvest_dictinary(
                        field,
                        iso_values,
                        package_dict,
                        schema,
                        default_language,
                        handled_fields,
                        source_config,
                    )

                self.handle_scheming_harvest_dictinary(
                    field,
                    iso_values,
                    extras,
                    package_dict,
                    default_language,
                    handled_fields,
                )

            # fall back to DOI URL for citation when the XML did not supply one.
            # The fluent handler above leaves citation = {default_language: ''} when
            # no value is found, so check for an empty/blank string and replace it.
            citation_val = package_dict.get("citation", {})
            if (
                isinstance(citation_val, dict)
                and not citation_val.get(default_language, "").strip()
            ):
                URIF = toolkit.h.cioos_get_fully_qualified_package_uri(
                    package_dict,
                    uri_field="unique-resource-identifier-full",
                    default_code_space="doi.org",
                )
                if URIF:
                    package_dict["citation"] = {default_language: URIF[0]}

            # set default values
            package_dict["progress"] = (
                package_dict.get("progress", "onGoing") or "onGoing"
            )
            package_dict["frequency-of-update"] = (
                package_dict.get("frequency-of-update", "asNeeded") or "asNeeded"
            )

        extras_as_list = []
        for key, value in extras.items():
            if package_dict.get(key, ""):
                log.error(
                    "extras %s found in package dict: key:%s value:%s", key, key, value
                )
                continue  # already stored as a schema field; adding it to extras too would
                # trigger CKAN's "There is a schema field with the same name" error
            if isinstance(value, (list, dict)):
                extras_as_list.append({"key": key, "value": json.dumps(value)})
            else:
                extras_as_list.append({"key": key, "value": value})

        package_dict["extras"] = extras_as_list

        ## Configuring Responsible Organization group
        group_mapping = source_config.get("organization_mapping", {})
        group_type = "resorg"
        # filter out entries with no organisation
        parties = [
            x
            for x in iso_values.get("cited-responsible-party", [])
            if x.get("organisation-name")
        ]
        # filter out entries with no organisation from metadata-point-of-contact
        additional_parties = [
            x
            for x in iso_values.get("metadata-point-of-contact", [])
            if x.get("organisation-name")
        ]
        # generate groups
        groups = handle_groups(
            context,
            harvest_object,
            group_mapping,
            group_type,
            parties,
            additional_parties,
        )
        if groups:
            # remove duplicates by populating dictionary and then converting to list
            package_dict["groups"] = list(
                {x["id"]: x for x in (package_dict.get("groups", []) + groups)}.values()
            )

        # update resource format and translated relevant fields
        resources = package_dict.get("resources", [])
        for resource in resources:
            url = resource.get("url", "").strip()
            protocol = resource.get("resource_locator_protocol") or resource.get(
                "protocol"
            )
            format = resource.get("format") or "text/html"
            if url:
                format = self.cioos_guess_resource_format(url) or format
            resource["format"] = format

        translate_resource_fields(resources, default_language, json_decoder=load_json)

        package_dict["resources"] = resources
        return self.trim_values(package_dict)

    def handle_fluent_harvest_dictinary(
        self,
        field,
        iso_values,
        package_dict,
        schema,
        default_language,
        handled_fields,
        harvest_config,
    ):
        do_clean = toolkit.asbool(harvest_config.get("clean_tags", False))
        handle_fluent_field(
            field,
            iso_values,
            package_dict,
            schema,
            default_language,
            handled_fields,
            harvest_config=harvest_config,
            tag_source=field["field_name"],
            tag_accessor=lambda t: load_json(t.get("keyword", t)),
            tag_sanitizer=munge.munge_tag if do_clean else None,
            merge_existing_tags=True,
            json_decoder=load_json,
        )

    def handle_composite_harvest_dictinary(
        self, field, iso_values, extras, package_dict, default_language, handled_fields
    ):
        handle_composite_field(
            field,
            iso_values,
            package_dict,
            handled_fields,
            separator=plugins.toolkit.h.scheming_composite_separator(),
            extras=extras,
        )

    def handle_scheming_harvest_dictinary(
        self, field, iso_values, extras, package_dict, default_language, handled_fields
    ):
        handle_scheming_field(
            field,
            iso_values,
            extras,
            package_dict,
            handled_fields,
        )
