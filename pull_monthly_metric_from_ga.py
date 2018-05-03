import time
from collections import OrderedDict
import pprint
from pull_web_stats_from_ga import initialize_ga_api, get_IDs, get_history_by_month, insert_zeros, push_dataset_to_ckan, field_mapper

import sys # These two lines are here to prevent a weird problem wherein
sys.excepthook = sys.__excepthook__ # a) the script would get stuck after
# failing to retrieve some data from Google Analytics after trying twice
# and b) would just hang, rather than raising an exception (as designed).
#http://stackoverflow.com/questions/12865637/why-doesnt-python-exit-from-a-raised-exception-when-executed-with-an-absolute-p

from credentials_file import server, monthly_downloads_resource_id
####### Get monthly downloads data ####################################
    # Create entire dataset-downloads dataset by looking at every month.
    # For every resource ID in the data.json file, run metric_by_month and upsert the results to the monthly-downloads datastore.

def fetch_and_store_metric(metric,metrics_name,target_resource_id,modify_datastore,event,first_yearmonth,limit=0):
    # target_resource_id is the resource ID of the dataset that the
    # fetched information should be sent to (not to be confused with
    # the resource IDs of the data files about which metrics are being
    # obtained).
    service = initialize_ga_api()
    from credentials_file import profile # The Google Analytics profile ID for data.wrpdc.org.

    metrics = ', '.join(metrics_name.keys())
    resources, packages, lookup_by_id = get_IDs()

    #Write the field names as the first line of the file:
    dmbm_file = 'dataset_'+metric+'_by_month.csv'
    fcsv = open(dmbm_file,'w')
    extra_fields = ['Year+month']
    extra_fields += ['Package','Resource','Publisher','Groups','Package ID','Resource ID']
        # This is the first place to add extra fields.
    #if metric == 'downloads':
    #    extra_fields.remove("Resource ID") # This causes an error.
    csv_row = ','.join(extra_fields + metrics_name.values())
    fcsv.write(csv_row+'\n')

    all_rows = []
    if limit > 0:
        resources = resources[:limit]
    for k,r_id in enumerate(resources):
        metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 1. ")
            metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 2. ")
            metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 3. ")
            raise Exception("Unable to get metric_by_month data for resource ID {} after trying thrice.".format(r_id))
        if 'rows' in metric_by_month:
            metric_rows = metric_by_month['rows']
            # Unfortunately, Google Analytics does not provide the resource
            # ID as a queryable parameter for pageviews or other metrics
            # the way it does for downloads (since the resource ID has been
            # inserted as the eventLabel).
            # Therefore, I need to manually insert the resource ID in these
            # cases (and now other parameters).

            lbid = lookup_by_id[r_id]

            new_metric_rows = []
            for row in metric_rows:
                if metric == 'downloads':
                    row.remove(unicode(r_id))
                new_metric_rows.append([row[0], lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id] + row[1:])
                # This is the second place to add (and order) extra fields.
            metric_rows = new_metric_rows

            metric_rows = insert_zeros(metric_rows,
                [lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id], len(metrics_name), first_yearmonth)
                # This is the third place to add extra fields.

            pprint.pprint(metric_rows)
            for row in metric_rows:
                csv_row = ','.join(row)
                fcsv = open(dmbm_file,'a')
                fcsv.write(csv_row+'\n')
                fcsv.close()

            all_rows += metric_rows
        else:
            print("No rows found in the response for the dataset with resource ID {}".format(r_id))
        time.sleep(1.0)

# Create an update to the dataset-downloads dataset by just looking at this month and last month and upserting the results.
    if modify_datastore:
        # The fourth and final place to add extra fields is field_mapper,
        # which is now defined in pull_web_stats_from_ga, but you can also
        # just extend it here with a command like
        #       field_mapper['Beeblebrox'] = "dict"
        keys = ['Year+month', 'Resource ID']
        push_dataset_to_ckan(all_rows, metrics_name, server, target_resource_id, field_mapper, keys, extra_fields) #This pushes everything in metric_rows

    fields = extra_fields + metrics_name.values()
    return all_rows, fields

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

def main():
    service = initialize_ga_api()
    #metrics_name = OrderedDict([("ga:totalEvents",'Downloads'),
    #                ("ga:uniqueEvents",'Unique downloads')
    #                ])
    #metrics = ', '.join(metrics_name.keys())
    #h = get_history_by_month(service,profile,metrics,'40776043-ad00-40f5-9dc8-1fde865ff571',False)
    #print h

    #raw_input('Press enter to continue: ')


    store = True
    metric = 'downloads'
    event = True
    store = False


    metric = 'pageviews'
    first_yearmonth = '201510'
    if metric == 'downloads':
        target_resource_id = monthly_downloads_resource_id
        first_yearmonth = '201603'
    elif metric == 'pageviews':
        target_resource_id = None
        event = False
        store = False
    if metric == 'downloads':
        metrics_name = OrderedDict([("ga:totalEvents",'Downloads'),
                        ("ga:uniqueEvents",'Unique downloads')
                        ])
    elif metric == 'pageviews':
        metrics_name = OrderedDict([("ga:pageviews",'Pageviews')
                        ])

    fetch_and_store_metric(metric,metrics_name,target_resource_id,store,event,first_yearmonth)

if __name__ == '__main__':
    main()
