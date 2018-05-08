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
    """Connection to the CKAN datastore"""

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

    def regulate_tags(self, package_id):
        # Format of 'tags' field in CKAN package parameters:
        # [{u'vocabulary_id': None, u'state': u'active',
        # u'display_name': u'cookie_monster', u'id':
        # u'e0fb1b91-a69c-4924-8930-725d8dea281a', u'name': u'cookie_monster'},
        # {u'vocabulary_id': None, u'state': u'active', u'display_name':
        # u'etl', u'id': u'41c116d5-8df4-4da1-87bc-77cd7e8d57ff', u'name': u'etl'}]

        # Get current tags:
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        metadata = ckan.action.package_show(id=package_id)
        current_tags = metadata['tags']

        # Adjust tags by getting rid of 'etl' and adding '_etl'.
        new_tags = [t for t in current_tags if t['name'] != 'etl']
        new_tags.append({'name': '_etl'})

        payload = {}
        payload['id'] = package_id
        payload['tags'] = new_tags
        results = ckan.action.package_patch(**payload)

    def adjust_metadata(self, resource_id):
        """
        It's necessary to update the metadata of the resource after creating the datastore.
        Otherwise the URL is wrong and data won't upsert.
        :param resource: Resource ID for which the metadata will be modified.
        :return: Whatever ckanapi returns.
        """
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        dump_url = "{}/datastore/dump/{}".format(self.site,resource_id)
        response = ckan.action.resource_patch(id=resource_id,
                url=dump_url,
                url_type='datanudger',
                last_modified=datetime.datetime.utcnow().isoformat())

        package_id = ckan.action.resource_show(id=resource_id)['package_id']
        self.regulate_tags(package_id)
        return response

    def create_datastore(self, resource_id, fields, keys=None):
        """
        Creates a new datastore for the specified resource.
        :param resource_id: Resource ID for which new datastore is being made.
        :param fields: Header fields for CSV file.
        :return: The newly created resource object.
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

        self.adjust_metadata(resource_id)
        print("Datastore created for resource ID {}.".format(resource_id))
        return response

    def delete_datastore(self, resource_id):
        """
        Deletes the datastore table for the resource with ID resource_id.
        :param resource: Resource to remove table from.
        :return: Whatever ckanapi returns.
        """
        # Note! When delete_datastore (or other operations) fail, 
        # check whether the 'url_type' of the resource is set to 
        # something other than 'datastore'. This can cause weird
        # problems.
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        try:
            response = ckan.action.datastore_delete(id=resource_id, force=True)
        except ckanapi.errors.NotFound:
            # If the datastore can't be found, an exception is thrown,
            # though not a very informative one.

            # Let's check if the resource itself exists.
            try:
                metadata = ckan.action.resource_show(id=resource_id)
            except ckanapi.errors.NotFound:
                raise ValueError("There is no resource on {} under resource ID {}.".format(self.site, resource_id))
            print("This resource does not have a datatore to delete.")
            response = "Non-existent datastore does not need to be deleted."

        return response

    def upsert(self, resource_id, data, method='insert'):
        """
        Upsert data into the datastore.
        :param resource_id: ID of the resource into which the data will be inserted/upserted/whatever.
        :param data: Data to be upserted.
        :return: Whatever ckanapi returns.
        """
        ckan = ckanapi.RemoteCKAN(self.site, apikey=self.key)
        response = ckan.action.datastore_upsert(resource_id = resource_id, records = data, method = method, force = True)
        return response
