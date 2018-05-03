# This is an older version of the script for pulling monthly download
# numbers for WPRDC resources from Google Analytics. It has been modified
# to continue working with the revised functions in pull_web_stats_from_ga.
# For now, it still produces the leaner 4-column data format which is all
# that the WPRDC dashboard really needs.

# The newer version of this script is pull_monthly_dls_from_ga.py, and
# it inserts all the other human-readable stuff many redundant times.
import time
from collections import OrderedDict
import pprint
from pull_web_stats_from_ga import initialize_ga_api, get_IDs, get_history_by_month, insert_zeros, push_dataset_to_ckan, field_mapper


import sys # These two lines are here to prevent a weird problem wherein
sys.excepthook = sys.__excepthook__ # a) the script would get stuck after
# failing to retrieve some data from Google Analytics after trying twice
# and b) would just hang, rather than raising an exception (as designed).
#http://stackoverflow.com/questions/12865637/why-doesnt-python-exit-from-a-raised-exception-when-executed-with-an-absolute-p


####### Get monthly downloads data ####################################
    # Create entire dataset-downloads dataset by looking at every month.
    # For every resource_id in the data.json file, run downloads_by_month and upsert the results to the monthly-downloads datastore.

def main():
    from credentials_file import profile, monthly_downloads_resource_id # The profile ID for data.wprdc.org.
    service = initialize_ga_api()

    modify_datastore = True
    
    #modify_datastore = False
    if not modify_datastore:
        print("NOT modifying the datastore.")

    server = "Live" #server = "Staging"

    metrics_name = OrderedDict([("ga:totalEvents",'Downloads'),
                    ("ga:uniqueEvents",'Unique downloads')
                    ])
    metrics = ', '.join(metrics_name.keys())


    resources, packages, lookup_by_id = get_IDs()

    #Write the field names as the first line of the file:
    ddbm_file = 'dataset_downloads_by_month.csv'
    fcsv = open(ddbm_file,'w')
    csv_row = ','.join(['Year+month'] + metrics_name.values())
    fcsv.write(csv_row+'\n')

    all_rows = []
    for k,r_id in enumerate(resources):
        downloads_by_month = get_history_by_month(service, profile, metrics, r_id, True)
        print("downloads_by_month = {} ".format(downloads_by_month))
        if downloads_by_month is None:
            print("Strike 1. ")
            downloads_by_month = get_history_by_month(service, profile, metrics, r_id, True)
        if downloads_by_month is None:
            print("Strike 2. ")
            downloads_by_month = get_history_by_month(service, profile, metrics, r_id, True)
        if downloads_by_month is None:
            print("Strike 3. ")
            raise Exception("Unable to get downloads_by_month data for resource ID {} after trying twice.".format(r_id))
        if 'rows' in downloads_by_month:
            download_rows = downloads_by_month['rows']
            download_rows = insert_zeros(download_rows,
                [r_id],len(metrics_name),'201603')

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
