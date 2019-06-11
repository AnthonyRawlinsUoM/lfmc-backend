from serve.lfmc.library.geoserver.catalog import Catalog
from serve.lfmc.library.geoserver.support import DimensionInfo

import serve.lfmc.config.debug as dev


class GeoServer:

    def __init__(self):
        self.catalog = Catalog(
            "http://geoserver:8080/geoserver/rest/", "admin", "geoserver")
        self.lfmc = self.catalog.get_workspace("lfmc")

    def add_to_catalog(self, layer_group, layer_name, path_to_netcdf):
        if dev.DEBUG:
            print('Got call to save to GeoServer Catalog.')
            print("Layer group is: %s", layer_group)
            print("NetCDF is here: %s", path_to_netcdf)

        # Add coverageStore if it doesn't already exist
        ft = self.catalog.create_coveragestore(
            layer_name, workspace=ws, data=path_to_netcdf)

        print('Retrieving Default Style')
        ft.default_style = self.geo_server.get_style(
            'lfmc:{}'.format(layer_group))

        # print('Retrieving Layer')
        # fts = self.geo_server.get_layer(layer_name)

        # Add layer for coverage store
        timeInfo = DimensionInfo("time", "true", "LIST", None, "ISO8601", None)
        ft.metadata = ({'time': timeInfo})
        print('Attempting to save new coveragestore')
        self.catalog.save(ft)

        # Add layer to layer group
        lg = self.catalog.get_layergroup(layer_group)
        lg.layers.append(layer_name)
        lg.styles.append("null")
        self.catalog.save(lg)

    def get_layer_group(self, name):
        return self.catalog.get_layergroup(name, self.lfmc)

    def get_workspace(self):
        return self.lfmc

    def get_(self):
        return self.catalog.get_resource()

    def get_availability(self):
        """
        Gets the temporal and spatial extents for each published layer and returns JSON suitable for consumption by
        Staging
        http://128.250.160.167:8080/geoserver/rest/workspaces/lfmc/layergroups/

        title is a string naming the Layer in a human-friendly way. For example, it should be suitable for display in a
        layer listing GUI.

        abstract is a string describing the Layer in more detail than the title.

        keywords is a list of short strings naming topics relevant to this dataset.

        enabled is a Boolean flag which may be set to False to stop serving a Resource without deleting it. If this is
        set to True then (assuming a corresponding enabled Layer exists) the Resource will be served.

        native_bbox is a list of strings indicating the bounding box of the dataset in its native projection (the
        projection used to actually store it in physical media.) The first four elements of this list will be the
        bounding box coordinates (in the order minx, maxx, miny, maxy) and the last element will either be an EPSG code
        for the projection (for example, “EPSG:4326”) or the WKT for a projection not defined in the EPSG database.

        latlon_bbox is a list of strings indicating the bounding box of the dataset in latitude/longitude coordinates.
        The first four elements of this list will be the bounding box coordinates (in the order minx, maxx, miny, maxy).
        The fifth element is optional and, if present, will always be “EPSG:4326”.

        projection is a string describing the projection GeoServer should advertise as the native one for the resource.
        The way this influences the actual values GeoServer will report for data from this resource are determined by
        the projection_policy.

        projection_policy is a string determining how GeoServer will interpret the projection setting. It may take three
        values:

            FORCE_DECLARED: the data from the underlying store is assumed to be in the projection specified
            FORCE_NATIVE: the projection setting is ignored and GeoServer will publish the projection as determined by
            inspecting the source data
            REPROJECT: GeoServer will reproject the data in the underlying source to the one specified
            These are enumerated as constants in the geoserver.support package.

        metadata_links is a list of links to metadata about the resource annotated with a MIME type string and a string
        identifying the metadata standard.
        :return:
        """
        workspace = 'lfmc'
