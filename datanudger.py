import requests
import json
import datetime
import ckanapi
from pprint import pprint

# The original Datapusher code was extracted at some point from 
#       https://github.com/WPRDC/wprdc-etl
# Datanudger is backward-compatible but uses ckanapi to do the 
# heavy lifting. This provides a minimal set of functions to 
# duplicate what Dataupusher operations were already being used.

class Datanudger:
    """Connection to ckan datastore"""

    def __init__(self, global_settings, server="Staging", etl_settings_file=None):
        if global_settings is not None:
            self.ckan_url = global_settings['URLs'][server]['CKAN']
            self.dump_url = global_settings['URLs'][server]['Dump']
            self.key = global_settings['API Keys'][server]

            self.site = '/'.join(self.ckan_url.split('/')[0:3]) # Needed by ckanapi
        else: # Get settings from etl_settings_file, using the wprdc-etl settings.json format.
            print("global_settings is not defined. Falling back to settings in {}".format(etl_settings_file))
            with open(etl_settings_file) as f:
                settings = json.load(f)
                self.key = settings["loader"][server]["ckan_api_key"]
                self.site = settings["loader"][server]["ckan_root_url"]
                self.package_id = settings["loader"][server]["package_id"]

    def create_datastore(self, resource_id, fields, keys=None):
        """
        Creates new datastore for specified resource
        :param resource_id: resource ID for which new datastore is being made
        :param fields: header fields for CSV file
        :return: resource ID, if successful
        """

        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        response = ckan.action.datastore_create(id=resource_id, fields=fields, primary_key=keys, force=True)
        # Returns:	The newly created data object (as a dict)

        # An example response looks like this:
        # {u'fields': [{u'id': u'Year+month', u'type': u'text'},
        #     {u'id': u'Users', u'type': u'int'},
        #     {u'id': u'Sessions', u'type': u'int'},
        #     {u'id': u'Pageviews', u'type': u'int'},
        #     {u'id': u'Pageviews per session', u'type': u'float'},
        #     {u'id': u'Average session duration (seconds)',
        #      u'type': u'float'}],
        #     u'method': u'insert',
        #     u'primary_key': u'Year+month',
        #     u'resource_id': u'7a7f86c5-015c-4bf5-abcf-ad2332c38813'}
        print("Datastore created for resource ID {}.".format(resource_id))
        return response

    def delete_datastore(self, resource_id):
        """
        Deletes datastore table for resource ID with value resource_id
        :param resource: resource to remove table from
        :return: request status
        """
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        response = ckan.action.datastore_delete(id=resource_id, force=True)
        return response

    def upsert(self, resource_id, data, method='insert'):
        """
        Upsert data into datastore
        :param resource_id: ID of the resource into which the data will be inserted/upserted/whatever.
        :param data: data to be upserted
        :return: request status
        """
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        response = ckan.action.datastore_upsert(resource_id = resource_id, records = data, method = method, force = True)
        return response
