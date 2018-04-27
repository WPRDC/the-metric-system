"""This code was based on the HelloAnalytics.py example for accessing the Google Analytics API, but has been modified to pull down particular statistics and then uploade them to a CKAN repository."""

import argparse

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from credentials_file import SERVICE_ACCOUNT_E_MAIL, profile, API_key # to set the SERVICE_ACCOUNT_E_MAIL constant, GA profile ID, and API key (temporarily)
from credentials_file import site, tracking_resource_id, site_stats_resource_id, monthly_downloads_resource_id
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from datetime import date, timedelta, datetime

from collections import OrderedDict, defaultdict
import pprint
import requests
import json
import time
from datapusher import Datapusher

import ckanapi

import pandas as pd

import sys # These two lines are here to prevent a weird problem wherein
sys.excepthook = sys.__excepthook__ # a) the script would get stuck after
# failing to retrieve some data from Google Analytics after trying twice
# and b) would just hang, rather than raising an exception (as designed).
#http://stackoverflow.com/questions/12865637/why-doesnt-python-exit-from-a-raised-exception-when-executed-with-an-absolute-p

# Since the rate that the data is being returned decreases as the script
# runs, I'm wondering whether Google Analytics is rate-limiting this a
# little, so I've decreased the frequency from 2 Hz to 1 Hz.

def upsert_data(dp,resource_id,data):
    # Upsert data to the CKAN datastore (as configured in dp) and the
    # given resource ID.

    # The format of the data variable is a list of dicts, where each
    # dict represents a row of the array, with the column name being
    # the key and the column value being the value.

    # The types of the columns are defined when the datastore is
    # created/recreated, in a command like this:
    # dp.create_datastore(resource_id, reordered_fields, keys)

    # which returns a result like this:
    # {u'fields': [{u'type': u'text', u'id': u'Year+month'}, {u'type': u'text', u'id': u'Package'}, {u'type': u'text', u'id': u'Resource'}, {u'type': u'text', u'id': u'Publisher'}, {u'type': u'text', u'id': u'Groups'}, {u'type': u'text', u'id': u'Package ID'}, {u'type': u'text', u'id': u'Resource ID'}, {u'type': u'int', u'id': u'Pageviews'}], u'method': u'insert', u'primary_key': [u'Year+month', u'Resource ID'], u'resource_id': u'3d6b60f4-f25a-4e93-94d9-730eed61f69c'}
    #fields_list =
    #OrderedDict([('Year+month', u'201612'), ('Package', u'Allegheny County Air Quality'), ('Resource', u'Hourly Air Quality Data'), ('Publisher', u'Allegheny County'), ('Groups', u'Environment'), ('Package ID', u'c7b3266c-adc6-41c0-b19a-8d4353bfcdaf'), ('Resource ID', u'15d7dbf6-cb3b-407b-ae01-325352deed5c'), ('Pageviews', u'0')])
    r = dp.upsert(resource_id, data, method='upsert')
    if r.status_code != 200:
        print(r.text)
    else:
        print("Data successfully stored.")
    print("Status code: {}".format(r.status_code))
    return r.status_code == 200

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.

  Returns:
    A service that is connected to the specified API.
  """

  credentials = ServiceAccountCredentials.from_p12_keyfile(
    service_account_email, key_file_location, scopes=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service


def get_first_profile_id(service):
  # Use the Analytics service object to get the first profile id.

  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    # Get the first Google Analytics account.
    account = accounts.get('items')[0].get('id')

    # Get a list of all the properties for the first account.
    properties = service.management().webproperties().list(
        accountId=account).execute()

    if properties.get('items'):
      # Get the first property id.
      property = properties.get('items')[0].get('id')

      # Get a list of all views (profiles) for the first property.
      profiles = service.management().profiles().list(
          accountId=account,
          webPropertyId=property).execute()

      if profiles.get('items'):
        # return the first view (profile) id.
        return profiles.get('items')[0].get('id')

  return None


def get_results(service, profile_id):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the number of sessions within the past seven days.
  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date='7daysAgo',
      end_date='today',
      #dimensions="ga:eventCategory,ga:eventLabel"
      #sort="-ga:totalEvents",
      #filters='ga:medium==organic',
      metrics='ga:sessions').execute()

def get_metrics(service, profile_id, metrics, start_date='30daysAgo',end_date='today', dimensions='',sort_by='',filters=''):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the specified metrics over the given date range.
  if sort_by == '':
      return service.data().ga().get(
          ids='ga:' + profile_id,
          start_date=start_date,
          end_date=end_date,
          dimensions=dimensions,
          metrics=metrics).execute()
  else:
      return service.data().ga().get(
          ids='ga:' + profile_id,
          start_date=start_date,
          end_date=end_date,
          dimensions=dimensions,
          sort=sort_by,
          #dimensions="ga:eventCategory,ga:eventLabel"
          #sort=sort_by"-ga:totalEvents",
          filters=filters,
          metrics=metrics).execute()

def metrics_for_last_month(service, profile, metrics):
    today = date.today()
    last_month = (date.today().month - 2 + 12) % 12 + 1
    year = date.today().year
    if last_month == 12:
        year -= 1
    start_date = str(year) + "-" + str(last_month).zfill(2) + "-01"
    end_datetime = today - timedelta(days = today.day)
    end_date = end_datetime.strftime("%Y-%m-%d")
    return get_metrics(service, profile, metrics, start_date, end_date)

def get_full_history(service,profile,metrics,resource_id):
    requests = get_metrics(service, profile, metrics, start_date='2015-10-15',end_date='yesterday', dimensions="ga:date,ga:eventLabel", sort_by="ga:date", filters="ga:eventCategory==CKAN%20Resource%20Download%20Request,ga:eventLabel=="+resource_id)
    return requests

def get_history_by_month(service,profile,metrics,resource_id=None,event=False):
    # If the user doesn't specify the resource ID (or the event flag is
    # set to False), download monthly stats. Otherwise, treat it as one
    # of those special "eventLabel/eventCategory" things.
    if resource_id is None:
        try:
            requests = service.data().ga().get(
                ids='ga:' + profile,
                start_date='2015-10-15',
                end_date='yesterday',
                dimensions="ga:yearMonth",
                sort="ga:yearMonth",
                metrics=metrics).execute()
        except:
            requests = None
    elif not event:
        # This plucks out the entire history for a parameter like pageviews
        # for a particular resource ID.
        try:
            requests = service.data().ga().get(
                ids='ga:' + profile,
                start_date='2015-10-15',
                end_date='yesterday',
                dimensions="ga:yearMonth",
                sort="ga:yearMonth",
                #dimensions="ga:yearMonth,ga:pagePath",
                #sort="ga:yearMonth,ga:pagePath",
                #ga:pagePath=~/dataset/.*/resource/40776043-ad00-40f5-9dc8-1fde865ff571 # sort of works, but includes weird paths like this
                # /dataset/311-data/resource/40776043-ad00-40f5-9dc8-1fde865ff571/view/aa82cd96-6ee8-4cf0-a9eb-5b626bf4d90d
                #filters=None,
                filters="ga:pagePath=~^/dataset/.*/resource/"+resource_id+"$;ga:pagePath!~^/dataset/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", # This complex filter has been confirmed for one dataset
                # to get the exact number of pageviews as generated by the
                # R dashboard code.

                # To AND filters together, separate them with a semicolon.
                # To OR filters together, separate them with a comma.
                #filters="ga:pagePath=~/dataset/.*/resource/40776043-ad00-40f5-9dc8-1fde865ff571$",#works
                # But also catches things like
                # /terms-of-use?came_from=/dataset/city-facilities-centroids/resource/9a5a52fd-fbe5-45b3-b6b2-c3bdaf6a2e04
                #filters="ga:pagePath=~^/dataset/.*/resource/9a5a52fd-fbe5-45b3-b6b2-c3bdaf6a2e04$", # actually works, although some weird isolated
                # hits like these get through:
                # /dataset/a8f7a1c2-7d4d-4daa-bc30-b866855f0419/resource/40776043-ad00-40f5-9dc8-1fde865ff571
                metrics=metrics).execute()
        except:
            requests = None
    else:
        # This is the special case for getting stats on something that
        # has an eventCategory saying it is a download stat and
        # an eventLabel identifying a particular resource by ID.
        try:
            requests = get_metrics(service, profile, metrics, start_date='2015-10-15', end_date='yesterday', dimensions="ga:yearMonth,ga:eventLabel,ga:eventCategory", sort_by="ga:yearMonth",
            filters="ga:eventLabel=="+resource_id)

            #filters="ga:eventCategory==CKAN%20Resource%20Download%20Request;ga:eventLabel=="+resource_id)
                # The double filter just doesn't work for some reason when the two filters are ANDed together.
                # Therefore I have devised the kluge in the else
                # statement below.
            # Using the following form seems to make no difference
            # (confirmed in GA Query Explorer):
            #filters="ga:eventCategory==CKAN%20Resource%20Download%20Request;ga:eventLabel=~^"+resource_id+"$")
        except:
            requests = None
        else:
            chs = requests['columnHeaders']
            xi = ['ga:eventCategory' in c.values() for c in chs].index(True)
            if 'rows' in requests:
                requests['rows'] = [r[:xi]+r[xi+1:] for r in requests['rows'] if 'CKAN API Request' not in r]

    return requests

def print_results(results, metrics = None):
  # Print data nicely for the user.
  if results:
    print('View (Profile): {}'.format(results.get('profileInfo').get('profileName')))
    if metrics is None:
        print('Total Sessions: {}'.format(results.get('rows')[0][0]))
    else:
        print('{} : {}'.format(metrics, results.get('rows')[0]))

  else:
    print('No results found')

def convert_results_into_dict(results,metrics_name):
    # (This function is for taking Google Analytics result and reformatting them.)
    # Keep dict values in string form because the dictionary
    # is intended to be passed to the ETL process.
    v = []
    cols = results['columnHeaders']
    rows = results['rows'][0]
    #columnHeaders is a list of dicts
    for k,col in enumerate(cols):
        metric = metrics_name[col['name']]
        if col['dataType'] in ['FLOAT','TIME']:
            v.append((metric,'{0:.2f}'.format(float(rows[k]))))
        else:
            v.append((metric,rows[k]))
    return OrderedDict(v)

def stats_to_dict(stats,columns):
    # The list stats is coming in as a list of strings (though
    # some represent numbers). This basically joins the names of the
    # metrics and the corresponding stats into an OrderedDict.
    tuples = []
    for k,col_name in enumerate(columns):
        tuples.append((col_name, stats[k]))
    return OrderedDict(tuples)

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def load_resource(site,resource_id,API_key):
    data = query_resource(site, 'SELECT * FROM "{}"'.format(resource_id), API_key)
    return data

def stringify_groups(p):
    groups_string = ''
    if 'groups' in p:
        groups = p['groups']
        groups_string = '|'.join(set([g['title'] for g in groups]))
    return groups_string

def get_IDs():
    # This function originally just got resource IDs (and other parameters) from the
    # current_package_list_with_resources API endpoint. However, this ignores 
    # resources that existed but have been deleted (or turned private again). To
    # track the statistics of these as well. We are now merging in historical 
    # resource IDs produced by dataset-tracker.
    resources, packages, = [], []
    lookup_by_id = defaultdict(lambda: defaultdict(str))
    url = "{}/api/3/action/current_package_list_with_resources?limit=99999".format(site)
    r = requests.get(url)
    # Traverse package list to get resource IDs.
    package_list = r.json()['result']
    for p in package_list:
        r_list = p['resources']
        if len(r_list) > 0:
            packages.append(r_list[0]['package_id'])
            for k,resource in enumerate(r_list):
                if 'id' in resource:
                    resources.append(resource['id'])
                    lookup_by_id[resource['id']]['package id'] = r_list[0]['package_id']
                    lookup_by_id[resource['id']]['package name'] = p['title']
                    lookup_by_id[resource['id']]['publisher'] = p['organization']['title']
                    lookup_by_id[resource['id']]['groups'] = stringify_groups(p)
                if 'name' in resource:
                    lookup_by_id[resource['id']]['name'] = resource['name']
#                else:
#                    lookup_by_id[resource['id']]['name'] = 'Unnamed resource'


    tracks = load_resource(site,tracking_resource_id,None)
    for r in tracks:
        r_id = r['resource_id']
        if r_id not in resources:
            if 'name' in resource:
                lookup_by_id[resource['id']]['name'] = resource['name']
                print("Adding resource ID {} ({})".format(r_id,r['resource_name']))
            else:
                print("Adding resource ID {} (Unnamed resource)".format(r_id))
            resources.append(r_id)
            if r['package_id'] not in packages:
                packages.append(r['package_id'])
            lookup_by_id[r_id]['package_id'] = r['package_id']
            lookup_by_id[r_id]['package name'] = r['package_name']
            lookup_by_id[r_id]['publisher'] = r['organization']
            lookup_by_id[r_id]['groups'] = stringify_groups(p)



    return resources, packages, lookup_by_id

def insert_zeros(rows,extra_columns,metrics_count,yearmonth = '201510'):
    # To make the data easier to work with, ensure that every dataset
    # has the same number of month entries, even if a bunch of them
    # are zero-downloads entries.
    # yearmonth is the first year/month in the dataset.
    first_year = int(yearmonth[0:4])
    first_month = int(yearmonth[4:6])
    today = date.today()
    last_year = today.year
    last_month = today.month

    new_rows = []
    year, month = first_year, first_month
    while year <= last_year:
        if year < last_year or month <= last_month:
            current_ym = '{:d}{:02d}'.format(year,month)
            yms = [row[0] for row in rows]
            if current_ym in yms:
                current_index = yms.index(current_ym)
                new_rows.append(rows[current_index])
            else:
                row_of_zeros = [unicode(current_ym)] + extra_columns
                base_length = len(row_of_zeros)
                for mtrc in range(0,metrics_count):
                    row_of_zeros.append(u'0')
                new_rows.append(row_of_zeros)
        month += 1
        if month == 13:
            year += 1
            month = 1
        if month == last_month and year == last_year and today.day == 1:
            year += 1 # Abort loop since Google Analytics has no data on
            # the first of the month. This is a bit of a kluge, but it works.
            # I am leaving this in to make the monthly-downloads sparklines
            # consistent with the behavior of the Users plot on the front
            # page of the dashboard.
    return new_rows

def group_by_1_sum_2_ax_3(all_rows,common_fields,fields_to_sum,eliminate,metrics_name):
    # Use pandas to group by common_fields (all fields that should be in
    # common and preserved), sum the elements in the sum field, and
    # eliminate the fields in eliminate, using metrics_name as a guideline.
    df = pd.DataFrame(all_rows,columns=list(metrics_name))
    df[fields_to_sum] = df[fields_to_sum].apply(pd.to_numeric)
    grouped = df.groupby(common_fields,as_index=False)[fields_to_sum].sum()
    for e in eliminate:
        if e in list(df):
            grouped.drop(e, axis=1, inplace=True)
    return grouped

def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    """Sets the given resource parameters to the given values for the specified
    resource.

    This fails if the parameter does not currently exist. (In this case, use
    create_resource_parameter()."""
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_resource_parameter(site,resource_id,p,API_key) for p in parameters]
        payload = {}
        payload['id'] = resource_id
        for parameter,new_value in zip(parameters,new_values):
            payload[parameter] = new_value
        #For example,
        #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
        results = ckan.action.resource_patch(**payload)
        print(results)
        print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))

    return success

def update_resource_timestamp(resource_id,field):
    return set_resource_parameters_to_values(site,package_id,[field],[datetime.now()],API_key)

def push_dataset_to_ckan(stats_rows, metrics_name, server, resource_id, field_mapper, keys, fields_to_add=[]):
    with open('ckan_settings.json') as f:
        settings = json.load(f)
    dp = Datapusher(settings, server=server)

    ### COMMENCE Code for initializing the datastore from scratch ###
    reordered_fields = []
    for f in fields_to_add:
        reordered_fields.append({"id": f, "type": field_mapper[f]})
    for heading in metrics_name.values():
        reordered_fields.append({"id": heading, "type": field_mapper[heading]})
    dp.delete_datastore(resource_id)
    print(dp.create_datastore(resource_id, reordered_fields, keys))
    ### TERMINATE Code for initializing the datastore from scratch ###
    fields_list = [d["id"] for d in reordered_fields]
    results_dicts = [stats_to_dict(r,fields_list) for r in stats_rows]
    if len(results_dicts) > 0:
        pprint.pprint(results_dicts[-1])
    success = upsert_data(dp,resource_id,results_dicts)
    if success:
        success2 = update_resource_timestamp(resource_id,'last_modified')
        return success2
    return success

def push_df_to_ckan(df, server, resource_id, field_mapper, all_fields, keys):
    with open('ckan_settings.json') as f:
        settings = json.load(f)
    dp = Datapusher(settings, server=server)

    ### COMMENCE Code for initializing the datastore from scratch ###
    ordered_fields = []
    for f in all_fields:
        ordered_fields.append({"id": f, "type": field_mapper[f]})

    dp.delete_datastore(resource_id)
    print(dp.create_datastore(resource_id, ordered_fields, keys))
    ### TERMINATE Code for initializing the datastore from scratch ###
    fields_list = [d["id"] for d in ordered_fields]
    list_of_dicts = df.to_dict(orient='records')
    results_dicts = []
    for d in list_of_dicts:
        results_dicts.append(OrderedDict((f,d[f]) for f in all_fields))
    pprint.pprint(results_dicts[-1])
    success = upsert_data(dp,resource_id,results_dicts)
    if success:
        success2 = update_resource_timestamp(resource_id,'last_modified')
        return success2
    return success

def initialize_ga_api():
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']

    # Use the developer console and replace the values with your
    # service account email and relative location of your key file.
    service_account_email = SERVICE_ACCOUNT_E_MAIL #'<Replace with your service account email address.>'
    key_file_location = 'Google-service-account-credentials.json' #'<Replace with /path/to/generated/client_secrets.p12>'

    # Authenticate and construct service.
    service = get_service('analytics', 'v3', scope, key_file_location,
    service_account_email)
    return service

def main():
    service = initialize_ga_api()

    metrics_name = OrderedDict([("ga:users",'Users'),
                    ("ga:sessions",'Sessions'),
                    ("ga:pageviews",'Pageviews'),
                    ("ga:pageviewsPerSession",'Pageviews per session'),
                    ("ga:avgSessionDuration",'Average session duration (seconds)')
                    ])
    metrics = ', '.join(metrics_name.keys())
#    results = metrics_for_last_month(service, profile, metrics)
#    results_dict = convert_results_into_dict(results,metrics_name)

    modify_datastore = True

    if not modify_datastore:
        print("NOT modifying the datastore.")

    server = "Live" #server = "Staging"
    if True:
        site_stats_file = 'site_stats_by_month.csv'
        stats_by_month = get_history_by_month(service, profile, metrics)
        if stats_by_month is None:
            stats_by_month = get_history_by_month(service, profile, metrics)
        if stats_by_month is None:
            raise Exception("Unable to get stats_by_month data after trying twice")
        stats_rows = stats_by_month['rows']
        pprint.pprint(stats_by_month['rows'])

        #Write the field names as the first line of the file:
        fcsv = open(site_stats_file,'w')
        csv_row = ','.join(['Year+month'] + metrics_name.values())
        fcsv.write(csv_row+'\n')

        for row in stats_rows:
            csv_row = ','.join(row)
            fcsv = open(site_stats_file,'a')
            fcsv.write(csv_row+'\n')
            fcsv.close()
        time.sleep(0.5)


        if modify_datastore:
            field_mapper = defaultdict(lambda: "float")
            field_mapper['Year+month'] = "text"
            field_mapper['Users'] = "int"
            field_mapper['Pageviews'] = "int"
            field_mapper['Sessions'] = "int"

            keys = 'Year+month'
            push_dataset_to_ckan(stats_rows, metrics_name, server, site_stats_resource_id, field_mapper, keys, [keys])

#########################################################################
    # [ ] Write a function to get dataset metrics.
    # Following the R code, we can pull all API requests for a given day (or date range)
    # and then split them by category (downloads have event category "CKAN Resource Download Request", while API calls have event category "CKAN API Request").
    metrics_name = OrderedDict([("ga:totalEvents",'Downloads'),
                    ("ga:uniqueEvents",'Unique downloads')
                    ])
    metrics = ', '.join(metrics_name.keys())
#    download_results = get_metrics(service, profile, metrics, start_date='30daysAgo',end_date='today', dimensions="ga:eventLabel", sort_by="-ga:totalEvents",filters="ga:eventCategory==CKAN Resource Download Request")
#    pprint.pprint(download_results)

    #downloads_by_day = get_metrics(service, profile, metrics, start_date='2015-10-15',end_date='today', dimensions="ga:yearMonth,ga:eventLabel", sort_by="ga:yearMonth",filters="ga:eventLabel==40776043-ad00-40f5-9dc8-1fde865ff571,ga:eventCategory==CKAN Resource Download Request")

#    downloads_by_day = get_metrics(service, profile, metrics, start_date='3daysAgo',end_date='today', dimensions="ga:date,ga:eventLabel", sort_by="ga:date",filters="ga:eventCategory==CKAN%20Resource%20Download%20Request,ga:eventLabel==40776043-ad00-40f5-9dc8-1fde865ff571")
#    pprint.pprint(downloads_by_day)
#    downloads_by_day = get_full_history(service, profile, metrics, "599faab9-3b05-469d-82fa-dcf11d58c2e7")
#   Despite the defaults (which are supposed to act as though include-empty-rows were true)
#   the search results DO NOT include empty rows:
#u'rows': [[u'20160309', u'599faab9-3b05-469d-82fa-dcf11d58c2e7', u'1', u'1'],
#           [u'20160311', u'599faab9-3b05-469d-82fa-dcf11d58c2e7', u'6', u'2'],
#           [u'20160322', u'599faab9-3b05-469d-82fa-dcf11d58c2e7', u'1', u'1'],
#    pprint.pprint(downloads_by_day)

# Maybe slicing still by dataset but just filling in the gaps with zeros would be the
# best solution. The main question is whether the absence of zeros is going to be harder
# to compensate on the front (pre-processing) or the back of the process (right
# before plotting).


####### Get monthly downloads data ####################################
    # Create entire dataset-downloads dataset by looking at every month.
    # For every resource_id in the data.json file, run downloads_by_month and upsert the results to the monthly-downloads datastore.

    if False:
        resources, packages = get_IDs()

        #Write the field names as the first line of the file:
        ddbm_file = 'dataset_downloads_by_month.csv'
        fcsv = open(ddbm_file,'w')
        csv_row = ','.join(['Year+month'] + metrics_name.values())
        fcsv.write(csv_row+'\n')

        all_rows = []
        for k,r_id in enumerate(resources):
            downloads_by_month = get_history_by_month(service, profile, metrics, r_id)
            if downloads_by_month is None:
                print("Strike 1. ",)
                downloads_by_month = get_history_by_month(service, profile, metrics, r_id)
            if downloads_by_month is None:
                print("Strike 2. ",)
                downloads_by_month = get_history_by_month(service, profile, metrics, r_id)
            if downloads_by_month is None:
                print("Strike 3. ",)
                raise Exception("Unable to get downloads_by_month data for resource ID {} after trying twice.".format(r_id))
            if 'rows' in downloads_by_month:
                download_rows = downloads_by_month['rows']
                download_rows = insert_zeros(download_rows,r_id,'201603')

                pprint.pprint(download_rows)
                for row in download_rows:
                    csv_row = ','.join(row)
                    fcsv = open(ddbm_file,'a')
                    fcsv.write(csv_row+'\n')
                    fcsv.close()

                all_rows += download_rows
            else:
                print("No rows found in the response for the dataset with resource ID {}.".format(r_id))
            time.sleep(1.0)

    # Create an update to the dataset-downloads dataset by just looking at this month and last month and upserting the results.
        if modify_datastore:
            resource_id = monthly_downloads_resource_id

            field_mapper = defaultdict(lambda: "int")
            field_mapper['Year+month'] = "text"
            field_mapper['Resource ID'] = "text"

            keys = ['Year+month', 'Resource ID']
            push_dataset_to_ckan(all_rows, metrics_name, server, resource_id, field_mapper, keys, keys) #This pushes everything in download_rows
        # [ ] Modify push_dataset_to_ckan to only initialize the datastore when necessary.
            # This script could have two modes:
            #   1) Download all data and overwrite the old stuff.
            #   2) Download only this month and last month and upsert into
            #   the existing repository.



#    get_full_history(resource_id="40776043-ad00-40f5-9dc8-1fde865ff571")
    # Pull down daily downloads/unique downloads/pageviews and then monthly
    # stats, and then use the data.json file to filter down to the things
    # we want to track (maybe).

    # If we dump the output
    # u'rows': [[u'40776043-ad00-40f5-9dc8-1fde865ff571', u'668', u'260'],
    #       [u'7a417847-37bb-4a16-a25e-477f2a71661d', u'493', u'82'],
    #       [u'c0fcc09a-7ddc-4f79-a4c1-9542301ef9dd', u'139', u'78'],
    #
    # and prepend some kind of date information, that could essentially be the
    # stuff inserted into the dataset (once the types have been properly taken care of)

if __name__ == '__main__':
  main()

field_mapper = defaultdict(lambda: "int")
field_mapper['Year+month'] = "text"
field_mapper['Package'] = "text"
field_mapper['Resource'] = "text"
field_mapper['Publisher'] = "text"
field_mapper['Groups'] = "text"
field_mapper['Package ID'] = "text"
field_mapper['Resource ID'] = "text"
field_mapper['Users'] = "int"
field_mapper['Pageviews'] = "int"
field_mapper['Sessions'] = "int"
