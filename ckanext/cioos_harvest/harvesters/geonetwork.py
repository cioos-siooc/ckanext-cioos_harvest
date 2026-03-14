import logging
from datetime import datetime

from ckan.logic import NotFound, get_action
from ckan.model import Session
from ckan.plugins.core import SingletonPlugin
from ckantoolkit import config

from ckan import model
from ckanext.cioos_harvest.harvesters.base import singleton_new
from ckanext.spatial.harvested_metadata import ISODocument, ISOElement
from ckanext.spatial.harvesters.csw import CSWHarvester

log = logging.getLogger(__name__)


# Extend the ISODocument definitions by adding some more useful elements

log.info("GeoNetwork harvester: extending ISODocument with TimeInstant")
ISODocument.elements.append(
    ISOElement(
        name="temporal-extent-instant",
        search_paths=[
            "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition/text()",
        ],
        multiplicity="*",
    )
)

# Some old GN instances still uses the old GML URL
# We'll add more xpath for addressing this issue
log.info("GeoNetwork harvester: adding old GML URI")
ISOElement.namespaces["oldgml"] = "http://www.opengis.net/gml"

for element in ISODocument.elements:
    newpaths = []

    for path in element.search_paths:
        if "gml:" in path:
            newpath = path.replace("gml:", "oldgml:")
            newpaths.append(newpath)

    for newpath in newpaths:
        element.search_paths.append(newpath)
        log.info("Added old URI for gml to %s", element.name)


class GeoNetworkHarvester(CSWHarvester, SingletonPlugin):
    __new__ = singleton_new

    def info(self):
        return {
            "name": "geonetwork",
            "title": "CSW server (GeoNetwork)",
            "description": "Harvests GeoNetwork instances via CSW",
            "form_config_interface": "Text",
        }

    def get_package_dict(self, iso_values, harvest_object):

        package_dict = super().get_package_dict(
            iso_values, harvest_object
        )

        # Add GeoNetwork specific extras
        gn_localized_url = harvest_object.job.source.url.strip("/")

        if gn_localized_url[-3:] == "csw":
            gn_localized_url = gn_localized_url[:-3]

        log.debug("GN localized URL %s", gn_localized_url)

        package_dict["extras"].append(
            {
                "key": "gn_view_metadata_url",
                "value": gn_localized_url
                + "/metadata.show?uuid="
                + harvest_object.guid,
            }
        )
        package_dict["extras"].append(
            {"key": "gn_localized_url", "value": gn_localized_url}
        )

        # Add other elements from ISO metadata
        time_extents = self.infer_timeinstants(iso_values)
        if time_extents:
            log.info("Adding Time Instants...")
            package_dict["extras"].append(
                {"key": "temporal-extent-instant", "value": time_extents}
            )

        ## Configuring package groups
        group_mapping = self.source_config.get("group_mapping", {})

        if group_mapping:
            groups = self.handle_groups(
                harvest_object, group_mapping, gn_localized_url, iso_values
            )
            if groups:
                package_dict["groups"] = groups

        if self.source_config.get("private_datasets") == "True":
            package_dict["private"] = True

        # Fix resources type according to resource_locator_protocol
        self.fix_resource_type(package_dict["resources"])

        # End of processing, return the modified package
        return package_dict

    def infer_timeinstants(self, values):
        extents = []

        for extent in values["temporal-extent-instant"]:
            if extent not in extents:
                extents.append(extent)

        log.info("%d TIME ISTANTS FOUND", len(extents))

        if len(extents) > 0:
            return ",".join(extents)

        return

    def handle_groups(self, harvest_object, group_mapping, gn_localized_url, values):
        try:
            context = {"model": model, "session": Session, "user": "harvest"}
            validated_groups = []
            cats = []

            harvest_iso_categories = self.source_config.get("harvest_iso_categories")
            if harvest_iso_categories == "True" or (
                harvest_iso_categories and harvest_iso_categories != "False"
            ):
                # Handle groups mapping using metadata TopicCategory
                cats = values["topic-category"]
                log.info("Topic categories: %r", cats)

            for cat in cats:
                groupname = group_mapping[cat]

                printname = groupname if not None else "NONE"
                log.debug("category %s mapped into %s" % (cat, printname))

                if groupname:
                    try:
                        data_dict = {"id": groupname}
                        get_action("group_show")(context, data_dict)
                        validated_groups.append({"name": groupname})
                    except NotFound:
                        log.warning(
                            "Group %s from category %s is not available"
                            % (groupname, cat)
                        )
        except Exception:
            log.warning("Error handling groups for metadata %s" % harvest_object.guid)

        return validated_groups

    def fix_resource_type(self, resources):
        for resource in resources:
            if "OGC:WMS" in resource["resource_locator_protocol"]:
                resource["format"] = "wms"

                if config.get("ckanext.spatial.harvest.validate_wms", False):
                    # Check if the service is a view service
                    url = resource["url"]
                    test_url = url.split("?")[0] if "?" in url else url
                    if self._is_wms(test_url):
                        resource["verified"] = True
                        resource["verified_date"] = datetime.now().isoformat()
