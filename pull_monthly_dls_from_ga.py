import time
from collections import defaultdict, OrderedDict
import pprint
from datapusher import Datapusher
from pull_monthly_metric_from_ga import fetch_and_store_metric
from pull_web_stats_from_ga import group_by_1_sum_2_ax_3, field_mapper, push_df_to_ckan

import sys # These two lines are here to prevent a weird problem wherein
sys.excepthook = sys.__excepthook__ # a) the script would get stuck after
# failing to retrieve some data from Google Analytics after trying twice
# and b) would just hang, rather than raising an exception (as designed).
#http://stackoverflow.com/questions/12865637/why-doesnt-python-exit-from-a-raised-exception-when-executed-with-an-absolute-p


####### Get monthly downloads data ####################################
    # Create entire downloads dataset by looking at every month.
    # For every resource_id in the data.json file, run metric_by_month and upsert the results to the monthly-downloads datastore.

    # One issue in trying to merge downloads and pageviews datasets is that there are a lot more resources that come up with non-zero pageviews than resources that come up with non-zero downloads (like 799 vs. like maybe 400-450).

def main():
    from credentials_file import profile # The profile ID for data.wrpdc.org.
    store = True
    metric = 'downloads'
    first_yearmonth = '201603'
    resource_id = 'e8889e36-e4b1-4343-bb51-fb687eb9a2ff'
    event = True
    #store = False
    metrics_name = OrderedDict([("ga:totalEvents",'Downloads'),
                    ("ga:uniqueEvents",'Unique downloads')
                    ])

    limit = 0
    if limit > 0:
        store = False
    all_rows, fields  = fetch_and_store_metric(metric,metrics_name,resource_id,store,event, first_yearmonth,limit)
    # Aggregate rows by package to get package stats.

    common_fields = ['Year+month','Package','Publisher','Groups','Package ID']
    fields_to_sum = ['Downloads', 'Unique downloads']
    df = group_by_1_sum_2_ax_3(all_rows, common_fields, fields_to_sum, [], fields)
    df = df.sort_values(by=['Package ID','Year+month'],ascending=[True,True])
    df = df.reset_index(drop=True) # Eliminate row numbers.
    if store:
        keys = ['Package ID','Year+month']
        all_fields = common_fields+fields_to_sum
        resource_id = 'd72725b1-f163-4378-9771-14ce9dad3002' # This is just
        # a temporary resource ID.
        push_df_to_ckan(df, "Live", resource_id, field_mapper, all_fields, keys)

    package_stats_file = 'package_downloads_by_month.csv'
    df.to_csv(package_stats_file, sep=',', line_terminator='\n', encoding='utf-8', index=False)

if __name__ == '__main__':
  main()
