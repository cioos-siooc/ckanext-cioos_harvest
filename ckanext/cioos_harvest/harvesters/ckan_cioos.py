"""CIOOS CKAN Harvester.

Harvests remote CKAN instances with improved handling/indexing of external
XML files, organization matching, field filtering, and package deletion
tracking.
"""

import collections
import json
import logging

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.search import SearchError
from ckan.logic import get_action

from ckan import model
from ckanext.cioos_harvest.harvesters.base import (
    all_packages_for_source,
    deduplicate_catalogue_entries,
    extract_xml_from_harvest_object,
    get_extra,
    get_object_extra,
    handle_groups,
    load_json,
    make_site_catalogue_entry,
    singleton_new,
)
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

log = logging.getLogger(__name__)


class CIOOSCKANHarvester(CKANHarvester):
    __new__ = singleton_new

    def info(self):
        return {
            "name": "ckan_cioos",
            "title": "CKAN CIOOS",
            "description": "Harvests remote CKAN instances with improved handling/indexing of external xml files and organization matching",
            "form_config_interface": "Text",
        }

    def validate_config(self, config):
        """
        Validate the harvest source configuration.

        Adds validation for field_filter_include and field_filter_exclude
        options which cannot be used together.
        """
        config = super().validate_config(config)
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if (
                "field_filter_include" in config_obj
                and "field_filter_exclude" in config_obj
            ):
                raise ValueError(
                    "Harvest configuration cannot contain both "
                    "field_filter_include and field_filter_exclude"
                )

        except ValueError as e:
            raise e

        return config

    def modify_search(self, pkg_dicts, remote_ckan_base_url, fq_terms):
        """
        Hook for subclasses to modify search results (e.g. spatial filtering).
        Default implementation returns results unchanged.
        """
        return pkg_dicts

    def gather_stage(self, harvest_job):
        """
        Override gather_stage to add field filtering and package deletion tracking.
        """
        log.debug("In CIOOSCKANHarvester gather_stage (%s)", harvest_job.source.url)
        toolkit.requires_ckan_version(min_version="2.0")
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_ckan_base_url = harvest_job.source.url.rstrip("/")

        # Filter in/out datasets from particular organizations
        fq_terms = []
        org_filter_include = self.config.get("organizations_filter_include", [])
        org_filter_exclude = self.config.get("organizations_filter_exclude", [])
        if org_filter_include:
            fq_terms.append(
                " OR ".join(
                    "organization:%s" % org_name for org_name in org_filter_include
                )
            )
        elif org_filter_exclude:
            fq_terms.extend(
                "-organization:%s" % org_name for org_name in org_filter_exclude
            )

        groups_filter_include = self.config.get("groups_filter_include", [])
        groups_filter_exclude = self.config.get("groups_filter_exclude", [])
        if groups_filter_include:
            fq_terms.append("groups:(%s)" % " OR ".join(groups_filter_include))
        elif groups_filter_exclude:
            fq_terms.append("-groups:(%s)" % " OR ".join(groups_filter_exclude))

        # Field filtering support
        field_filter_include = self.config.get("field_filter_include", [])
        field_filter_exclude = self.config.get("field_filter_exclude", [])
        if field_filter_include:
            result = collections.defaultdict(list)
            for item in field_filter_include:
                result[item["field"]].append(item["value"])
            fq_terms.append(
                " OR ".join(
                    "%s:(%s)" % (key, " OR ".join(result[key])) for key in result.keys()
                )
            )
        elif field_filter_exclude:
            result = collections.defaultdict(list)
            for item in field_filter_exclude:
                result[item["field"]].append(item["value"])
            fq_terms.extend(
                "-%s:(%s)" % (key, " OR ".join(result[key])) for key in result.keys()
            )

        # Ideally we can request from the remote CKAN only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self.last_error_free_job(harvest_job)
        log.debug("Last error-free job: %r", last_error_free_job)
        if last_error_free_job and not self.config.get("force_all", False):
            get_all_packages = False

            # Request only the datasets modified since
            last_time = last_error_free_job.gather_started
            # Note: SOLR works in UTC, and gather_started is also UTC, so
            # this should work as long as local and remote clocks are
            # relatively accurate. Going back a little earlier, just in case.
            import datetime

            get_changes_since = (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info("Searching for datasets modified since: %s UTC", get_changes_since)

            fq_since_last_time = f"metadata_modified:[{get_changes_since}Z TO *]"

            try:
                pkg_dicts = self._search_for_datasets(
                    remote_ckan_base_url, fq_terms + [fq_since_last_time]
                )

                # Call modify_search hook if available
                pkg_dicts = self.modify_search(
                    pkg_dicts, remote_ckan_base_url, fq_terms + [fq_since_last_time]
                )

            except SearchError as e:
                log.info(
                    "Searching for datasets changed since last time gave an error: %s",
                    e,
                )
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info(
                    "No datasets have been updated on the remote "
                    "CKAN instance since the last harvest job %s",
                    last_time,
                )
                return []

        # Fall-back option - request all the datasets from the remote CKAN
        to_delete_pkg = []
        if get_all_packages:
            # Request all remote packages
            try:
                pkg_dicts = self._search_for_datasets(remote_ckan_base_url, fq_terms)

                # Call modify_search hook if available
                pkg_dicts = self.modify_search(
                    pkg_dicts, remote_ckan_base_url, fq_terms
                )

            except SearchError as e:
                log.info("Searching for all datasets gave an error: %s", e)
                self._save_gather_error(
                    "Unable to search remote CKAN for datasets:%s url:%s"
                    "terms:%s" % (e, remote_ckan_base_url, fq_terms),
                    harvest_job,
                )
                return None

            # Track packages for deletion (no longer on remote)
            all_remote_pkg_ids = set([x["id"] for x in pkg_dicts])
            # get all local packages for this harvest source
            all_local_source_pkg = all_packages_for_source(harvest_job.source.id)
            all_local_source_pkg_ids = set([x["id"] for x in all_local_source_pkg])
            # id's of packages no longer available on remote
            to_delete_id = all_local_source_pkg_ids - all_remote_pkg_ids
            # packages no longer available on remote
            to_delete_pkg = [x for x in all_local_source_pkg if x["id"] in to_delete_id]

        if not pkg_dicts:
            self._save_gather_error(
                "No datasets found at CKAN: %s" % remote_ckan_base_url, harvest_job
            )
            return []

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []

            # Create harvest objects for packages to delete
            for pkg_dict in to_delete_pkg:
                if pkg_dict["id"] in package_ids:
                    log.info(
                        "Discarding duplicate dataset %s - probably due "
                        "to datasets being changed at the same time as "
                        "when the harvester was paging through",
                        pkg_dict["id"],
                    )
                    continue
                package_ids.add(pkg_dict["id"])

                log.debug(
                    'Creating HarvestObject for %s %s with status "delete"',
                    pkg_dict["name"],
                    pkg_dict["id"],
                )
                obj = HarvestObject(
                    guid=pkg_dict["id"],
                    extras=[HOExtra(key="status", value="delete")],
                    job=harvest_job,
                    content=json.dumps(pkg_dict),
                )
                obj.save()
                object_ids.append(obj.id)

            # Process rest of datasets
            for pkg_dict in pkg_dicts:
                if pkg_dict["id"] in package_ids:
                    log.info(
                        "Discarding duplicate dataset %s - probably due "
                        "to datasets being changed at the same time as "
                        "when the harvester was paging through",
                        pkg_dict["id"],
                    )
                    continue
                package_ids.add(pkg_dict["id"])

                log.debug(
                    "Creating HarvestObject for %s %s", pkg_dict["name"], pkg_dict["id"]
                )
                obj = HarvestObject(
                    guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            self._save_gather_error("%r" % str(e), harvest_job)

    def import_stage(self, harvest_object):
        """
        Override import_stage to handle package deletion.
        """
        log.debug("In CIOOSCKANHarvester import_stage")

        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }

        if not harvest_object:
            log.error("No harvest object received")
            return False

        if harvest_object.content is None:
            self._save_object_error(
                "Empty content for object %s" % harvest_object.id,
                harvest_object,
                "Import",
            )
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            # Check if this is a delete operation
            status = get_object_extra(harvest_object, "status")
            if status == "delete":
                # Delete package
                context = base_context.copy()
                context.update(
                    {
                        "ignore_auth": True,
                    }
                )
                get_action("package_delete")(context, {"id": package_dict["id"]})
                log.info("Deleted package {0}".format(package_dict["id"]))
                return True

            # Call parent import_stage for normal processing
            return super().import_stage(harvest_object)

        except Exception as e:
            log.exception(e)
            self._save_object_error("%s" % e, harvest_object, "Import")
            return False

    def modify_remote_organization(self, remote_org_id, pkg_dict, context):
        try:
            package_org = pkg_dict.get("organization")
            if package_org and package_org.get("id") == remote_org_id:
                remote_org_id = package_org.get("name", remote_org_id)

            # if there is a organization uri then try to match on that
            # get first item from organization-uri list if it exists
            uri = next(iter(package_org.get("organization-uri", [])), {})
            # we assume uri code is unique
            code = uri.get("code")
            if code:
                data_dict = {"fq": "organization-uri:%s" % code.replace(":", "_")}
                org = toolkit.get_action("organization_list")(
                    context.copy(), data_dict=data_dict
                )
                if org:
                    remote_org_id = org[0]
        except Exception as e:
            log.exception(e)
            raise
        return remote_org_id

    def modify_package_dict(self, package_dict, harvest_object):
        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }
        try:
            # convert extras key:value list to dictionary
            extras = {x["key"]: x["value"] for x in package_dict.get("extras", [])}
            package_dict = extract_xml_from_harvest_object(package_dict, harvest_object)

            if not extras.get("metadata_created_source"):
                extras["metadata_created_source"] = package_dict.get("metadata_created")
            if not extras.get("metadata_modified_source"):
                extras["metadata_modified_source"] = package_dict.get(
                    "metadata_modified"
                )

            # populate harvest source organization
            harvest_source = toolkit.get_action("harvest_source_show")(
                data_dict={"id": harvest_object.source.id}
            )
            extras["harvest_source_organization"] = harvest_source.get("organization")

            # convert extras back to a list of key/value dictionaries
            extras_as_list = []
            for key, value in extras.items():
                if package_dict.get(key, ""):
                    log.error(
                        "extras %s found in package dict: key:%s value:%s",
                        key,
                        key,
                        value,
                    )
                if isinstance(value, (list, dict)):
                    extras_as_list.append({"key": key, "value": json.dumps(value)})
                else:
                    extras_as_list.append({"key": key, "value": value})

            package_dict["extras"] = extras_as_list

            # provide default values if harvesting from a ckan catalogue that does not have these in their schema
            if not package_dict.get("projects"):
                package_dict["projects"] = []
            if not package_dict.get("datacentre"):
                package_dict["datacentre"] = []

            # add uri for dcat if it dosn't exist
            package_uri = (
                toolkit.config.get("ckan.site_url")
                + "/dataset/"
                + package_dict.get("name")
            )
            existing_extra = get_extra("uri", package_dict)
            if not existing_extra:
                extras_as_list.append({"key": "uri", "value": package_uri})

            # populate publishing data catalogue list
            source_dc = {
                "name": self.config.get("source_title")
                or harvest_object.job.source.title,
                "description": self.config.get("source_description"),
                "url": harvest_object.job.source.url.strip("/"),
            }
            dc = make_site_catalogue_entry()

            if not package_dict.get("included_in_data_catalogue"):
                package_dict["included_in_data_catalogue"] = [source_dc, dc]
            else:
                package_dict["included_in_data_catalogue"].append(dc)
                package_dict["included_in_data_catalogue"] = (
                    deduplicate_catalogue_entries(
                        package_dict["included_in_data_catalogue"]
                    )
                )

            # fix common schema fields errors
            schema = plugins.toolkit.h.scheming_get_dataset_schema("dataset")
            for field in schema["dataset_fields"]:
                if "repeating_subfields" in field:
                    field_name = field["field_name"]
                    value = package_dict.get(field_name)
                    if value == "":
                        value = []
                        package_dict[field_name] = value
                    elif value:
                        value = load_json(value)
                        if isinstance(value, dict):
                            value = [value]
                        package_dict[field_name] = value

            # condense uri into uri.code to make downstream templating easier
            # DOI
            URIF = toolkit.h.cioos_get_fully_qualified_package_uri(
                package_dict,
                uri_field="unique-resource-identifier-full",
                default_code_space="doi.org",
            )
            if URIF:
                if isinstance(package_dict["unique-resource-identifier-full"], list):
                    for index, item in enumerate(
                        package_dict["unique-resource-identifier-full"]
                    ):
                        package_dict["unique-resource-identifier-full"][index][
                            "code"
                        ] = URIF[index]
                else:
                    package_dict["unique-resource-identifier-full"]["code"] = URIF[0]

            # Organization URI
            organization = package_dict.get("organization")
            if organization:
                if isinstance(organization, list):
                    organization = organization[0]
                code = toolkit.h.cioos_get_fully_qualified_package_uri(
                    organization, uri_field="organization-uri"
                )
                organization["code"] = next(iter(code or []), "")
                package_dict["organization"] = organization

            # metadata-point-of-contact Individual and Organisation URI
            mpocs = package_dict.get("metadata-point-of-contact", [])
            for mpoc in mpocs:
                code = toolkit.h.cioos_get_fully_qualified_package_uri(
                    mpoc, uri_field="individual-uri_"
                )
                mpoc["individual-uri_code"] = next(iter(code or []), "")

                code = toolkit.h.cioos_get_fully_qualified_package_uri(
                    mpoc, uri_field="organisation-uri_"
                )
                mpoc["organisation-uri_code"] = next(iter(code or []), "")
            package_dict["metadata-point-of-contact"] = mpocs

            # cited-responsible-party Individual and Organisation URI
            crps = package_dict.get("cited-responsible-party", [])
            for crp in crps:
                code = toolkit.h.cioos_get_fully_qualified_package_uri(
                    crp, uri_field="individual-uri_"
                )
                mpoc["individual-uri_code"] = next(iter(code or []), "")

                code = toolkit.h.cioos_get_fully_qualified_package_uri(
                    crp, uri_field="organisation-uri_"
                )
                mpoc["organisation-uri_code"] = next(iter(code or []), "")
            package_dict["cited-responsible-party"] = crps

            if len(package_dict["tags"]) > 0:
                log.warning(
                    "Setting tags to an empty list. the following tags will be lost if not already added to keywords: %r",
                    package_dict["tags"],
                )
            package_dict["tags"] = []

            source_config = json.loads(harvest_object.source.config or "{}")
            ## Configuring Responsible Organization group
            group_mapping = source_config.get("organization_mapping", {})
            group_type = "resorg"
            # filter out entries with no organisation
            parties = [
                x
                for x in package_dict.get("cited-responsible-party", [])
                if x.get("organisation-name")
            ]
            # filter out entries with no organisation from metadata-point-of-contact
            additional_parties = [
                x
                for x in package_dict.get("metadata-point-of-contact", [])
                if x.get("organisation-name")
            ]
            # generate groups if not already set
            if package_dict.get("groups"):
                log.debug("Groups Found. Skipping Responable Organization processing.")
            else:
                groups = handle_groups(
                    base_context,
                    harvest_object,
                    group_mapping,
                    group_type,
                    parties,
                    additional_parties,
                )
                if groups:
                    # remove duplicates by populating dictionary and then converting to list
                    package_dict["groups"] = list(
                        {
                            x["id"]: x
                            for x in (package_dict.get("groups", []) + groups)
                        }.values()
                    )

            for resource in package_dict.get("resources", []):
                res_name = resource.get("name")
                res_name_translated = resource.get("name_translated")
                # populate multilingual resource name if not set
                if not res_name_translated:
                    res_name = load_json(res_name)
                    if isinstance(res_name, dict):
                        resource["name_translated"] = res_name
                        resource["name"] = res_name.get("en") or next(
                            iter(res_name.values()), resource.get("name", "")
                        )
                    else:
                        resource["name_translated"] = {}
                        resource["name_translated"]["en"] = res_name
                        resource["name_translated"]["fr"] = res_name

                res_desc = resource.get("description")
                res_desc_translated = resource.get("description_translated")
                # populate multilingual resource description if not set
                if not res_desc_translated:
                    res_desc = load_json(res_desc)
                    if isinstance(res_desc, dict):
                        resource["description_translated"] = res_desc
                        resource["description"] = res_desc.get("en") or next(
                            iter(res_desc.values()), resource.get("description", "")
                        )
                    else:
                        resource["description_translated"] = {}
                        resource["description_translated"]["en"] = res_desc
                        resource["description_translated"]["fr"] = res_desc

                if not resource.get("created_source"):
                    resource["created_source"] = resource.get("created")

                if not resource.get("metadata_modified_source"):
                    resource["metadata_modified_source"] = resource.get(
                        "metadata_modified"
                    )

        except Exception as e:
            log.exception(e)
            raise
        return package_dict
